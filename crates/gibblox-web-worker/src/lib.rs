#[cfg(target_arch = "wasm32")]
mod wasm {
    use futures_channel::oneshot;
    use futures_util::{FutureExt, future::LocalBoxFuture, future::select};
    use gibblox_android_sparse::{AndroidSparseBlockReader, AndroidSparseBlockReaderConfig};
    use gibblox_blockreader_messageport::{
        MessagePortBlockReaderClient, MessagePortBlockReaderServer,
    };
    use gibblox_cache::CachedBlockReader;
    use gibblox_cache_store_opfs::OpfsCacheOps;
    use gibblox_casync::{CasyncBlockReader, CasyncReaderConfig};
    use gibblox_casync_web::{
        WebCasyncChunkStore, WebCasyncChunkStoreConfig, WebCasyncIndexSource,
    };
    use gibblox_core::{
        BlockReader, GibbloxError, GibbloxErrorKind, GibbloxResult, GptBlockReader,
        GptPartitionSelector,
    };
    use gibblox_http::{HttpBlockReader, HttpBlockReaderConfig};
    use gibblox_mbr::{MbrBlockReader, MbrBlockReaderConfig, MbrPartitionSelector};
    use gibblox_pipeline::{
        PipelineSource, PipelineSourceCasyncSource, decode_pipeline, pipeline_identity_id,
        pipeline_identity_string, validate_pipeline,
    };
    use gibblox_xz::{XzBlockReader, XzBlockReaderConfig};
    use gloo_timers::future::sleep;
    use js_sys::{Array, Object, Reflect, Uint8Array};
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tracing::{debug, error, info, warn};
    use url::Url;
    use wasm_bindgen::{JsCast, JsValue, closure::Closure};
    use wasm_bindgen_futures::spawn_local;
    use web_sys::{DedicatedWorkerGlobalScope, MessageChannel, MessageEvent, MessagePort, Worker};

    const READY_TIMEOUT: Duration = Duration::from_secs(5);
    const WORKER_PROTOCOL_VERSION: u32 = 1;
    const PIPELINE_BLOCK_SIZE: u32 = 512;

    const READY_CMD: &str = "ready";
    const ERROR_CMD: &str = "error";
    const OPEN_PIPELINE_CMD: &str = "open_pipeline";
    const SHUTDOWN_CMD: &str = "shutdown";

    #[derive(Clone, Debug)]
    pub struct WorkerMetadata {
        pub protocol_version: u32,
    }

    pub struct OpenPipelineResult {
        pub identity: String,
        pub size_bytes: u64,
        pub reader: MessagePortBlockReaderClient,
    }

    #[derive(Clone)]
    struct SendJsValue(JsValue);

    unsafe impl Send for SendJsValue {}
    unsafe impl Sync for SendJsValue {}

    impl SendJsValue {
        fn as_js_value(&self) -> &JsValue {
            &self.0
        }
    }

    type PendingMap = Arc<Mutex<BTreeMap<u32, oneshot::Sender<GibbloxResult<SendJsValue>>>>>;

    pub struct GibbloxWebWorker {
        worker: Worker,
        metadata: WorkerMetadata,
        pending: PendingMap,
        next_id: AtomicU32,
        _on_message: Closure<dyn FnMut(MessageEvent)>,
        _on_error: Closure<dyn FnMut(web_sys::Event)>,
    }

    impl GibbloxWebWorker {
        pub async fn new(worker: Worker) -> GibbloxResult<Self> {
            let (ready_tx, ready_rx) = oneshot::channel::<GibbloxResult<WorkerMetadata>>();
            let ready_tx = Rc::new(RefCell::new(Some(ready_tx)));
            let pending: PendingMap = Arc::new(Mutex::new(BTreeMap::new()));

            let ready_tx_for_message = Rc::clone(&ready_tx);
            let pending_for_message = Arc::clone(&pending);
            let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
                let data = event.data();

                if let Some(id) = prop_u32_opt(&data, "id") {
                    let tx = pending_for_message
                        .lock()
                        .ok()
                        .and_then(|mut map| map.remove(&id));
                    let Some(tx) = tx else {
                        return;
                    };

                    if prop_bool(&data, "ok").unwrap_or(false) {
                        let _ = tx.send(Ok(SendJsValue(data)));
                    } else {
                        let _ = tx.send(Err(response_error(&data)));
                    }
                    return;
                }

                let Ok(cmd) = prop_string(&data, "cmd") else {
                    return;
                };
                match cmd.as_str() {
                    READY_CMD => {
                        let _ = try_send_ready_once(
                            &ready_tx_for_message,
                            parse_worker_metadata(&data).map_err(|err| {
                                GibbloxError::with_message(
                                    GibbloxErrorKind::Io,
                                    format!("gibblox worker ready payload invalid: {err}"),
                                )
                            }),
                        );
                    }
                    ERROR_CMD => {
                        let message = prop_string(&data, "error")
                            .unwrap_or_else(|_| "unknown worker startup error".to_string());
                        let startup_sent = try_send_ready_once(
                            &ready_tx_for_message,
                            Err(GibbloxError::with_message(
                                GibbloxErrorKind::Io,
                                format!("gibblox worker startup failed: {message}"),
                            )),
                        );
                        if !startup_sent {
                            reject_all_pending(
                                &pending_for_message,
                                GibbloxError::with_message(
                                    GibbloxErrorKind::Io,
                                    format!("gibblox worker error: {message}"),
                                ),
                            );
                        }
                    }
                    _ => {}
                }
            });
            worker.set_onmessage(Some(on_message.as_ref().unchecked_ref()));

            let ready_tx_for_error = Rc::clone(&ready_tx);
            let pending_for_error = Arc::clone(&pending);
            let on_error =
                Closure::<dyn FnMut(web_sys::Event)>::new(move |event: web_sys::Event| {
                    let message = format!(
                        "gibblox worker transport error: {}",
                        js_value_to_string(event.into())
                    );
                    let startup_sent = try_send_ready_once(
                        &ready_tx_for_error,
                        Err(GibbloxError::with_message(
                            GibbloxErrorKind::Io,
                            message.clone(),
                        )),
                    );
                    if !startup_sent {
                        reject_all_pending(
                            &pending_for_error,
                            GibbloxError::with_message(GibbloxErrorKind::Io, message),
                        );
                    }
                });
            worker.set_onerror(Some(on_error.as_ref().unchecked_ref()));

            let ready_future = async {
                ready_rx.await.map_err(|_| {
                    GibbloxError::with_message(
                        GibbloxErrorKind::Io,
                        "gibblox worker readiness channel closed",
                    )
                })?
            }
            .fuse();
            let timeout = sleep(READY_TIMEOUT).fuse();
            futures_util::pin_mut!(ready_future, timeout);

            let metadata = match select(ready_future, timeout).await {
                futures_util::future::Either::Left((result, _)) => result?,
                futures_util::future::Either::Right((_, _)) => {
                    worker.set_onmessage(None);
                    worker.set_onerror(None);
                    worker.terminate();
                    return Err(GibbloxError::with_message(
                        GibbloxErrorKind::Io,
                        "timed out waiting for gibblox worker ready state",
                    ));
                }
            };

            if metadata.protocol_version != WORKER_PROTOCOL_VERSION {
                worker.set_onmessage(None);
                worker.set_onerror(None);
                worker.terminate();
                return Err(GibbloxError::with_message(
                    GibbloxErrorKind::Unsupported,
                    format!(
                        "gibblox worker protocol mismatch: host={WORKER_PROTOCOL_VERSION} worker={}",
                        metadata.protocol_version
                    ),
                ));
            }

            Ok(Self {
                worker,
                metadata,
                pending,
                next_id: AtomicU32::new(1),
                _on_message: on_message,
                _on_error: on_error,
            })
        }

        pub fn metadata(&self) -> &WorkerMetadata {
            &self.metadata
        }

        pub async fn open_pipeline(
            &self,
            pipeline_bytes: &[u8],
        ) -> GibbloxResult<OpenPipelineResult> {
            let len = u32::try_from(pipeline_bytes.len()).map_err(|_| {
                GibbloxError::with_message(
                    GibbloxErrorKind::OutOfRange,
                    "pipeline payload exceeds browser transfer limits",
                )
            })?;

            let request = Object::new();
            set_prop(
                &request,
                "cmd",
                JsValue::from_str(OPEN_PIPELINE_CMD),
                "build open_pipeline request",
            )?;
            let bytes = Uint8Array::new_with_length(len);
            bytes.copy_from(pipeline_bytes);
            set_prop(
                &request,
                "pipeline_bin",
                bytes.into(),
                "build open_pipeline request",
            )?;

            let response = self.request(request, None).await?;
            let identity = prop_string(response.as_js_value(), "identity")?;
            let size_bytes = prop_u64_string(response.as_js_value(), "size_bytes")?;
            let port = prop_message_port(response.as_js_value(), "port")?;
            let reader = MessagePortBlockReaderClient::connect(port).await?;

            Ok(OpenPipelineResult {
                identity,
                size_bytes,
                reader,
            })
        }

        pub async fn shutdown(&self) -> GibbloxResult<()> {
            let request = Object::new();
            set_prop(
                &request,
                "cmd",
                JsValue::from_str(SHUTDOWN_CMD),
                "build shutdown request",
            )?;
            self.request(request, None).await.map(|_| ())
        }

        pub fn terminate(&self) {
            self.worker.terminate();
            reject_all_pending(
                &self.pending,
                GibbloxError::with_message(GibbloxErrorKind::Io, "gibblox worker terminated"),
            );
        }

        async fn request(
            &self,
            request: Object,
            transfer: Option<Array>,
        ) -> GibbloxResult<SendJsValue> {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            set_prop(
                &request,
                "id",
                JsValue::from_f64(id as f64),
                "build gibblox worker request",
            )?;

            let (tx, rx) = oneshot::channel();
            {
                let mut pending = self.pending.lock().map_err(|_| {
                    GibbloxError::with_message(
                        GibbloxErrorKind::Io,
                        "gibblox worker pending map lock poisoned",
                    )
                })?;
                pending.insert(id, tx);
            }

            let request: JsValue = request.into();
            let post_result = if let Some(transfer) = transfer {
                self.worker
                    .post_message_with_transfer(&request, transfer.as_ref())
            } else {
                self.worker.post_message(&request)
            };

            if let Err(err) = post_result {
                if let Ok(mut pending) = self.pending.lock() {
                    let _ = pending.remove(&id);
                }
                return Err(js_io(err));
            }

            match rx.await {
                Ok(result) => result,
                Err(_) => Err(GibbloxError::with_message(
                    GibbloxErrorKind::Io,
                    "gibblox worker response channel closed",
                )),
            }
        }
    }

    impl Drop for GibbloxWebWorker {
        fn drop(&mut self) {
            self.worker.set_onmessage(None);
            self.worker.set_onerror(None);
            reject_all_pending(
                &self.pending,
                GibbloxError::with_message(GibbloxErrorKind::Io, "gibblox worker dropped"),
            );
        }
    }

    #[derive(Default)]
    struct WorkerState {
        readers: BTreeMap<String, Arc<dyn BlockReader>>,
        servers: Vec<MessagePortBlockReaderServer>,
    }

    pub fn run_if_worker(worker_name: &str) -> bool {
        let Ok(scope) = js_sys::global().dyn_into::<DedicatedWorkerGlobalScope>() else {
            return false;
        };
        if !worker_name.is_empty() && scope.name() != worker_name {
            return false;
        }

        run_worker(scope);
        true
    }

    pub fn run_worker(scope: DedicatedWorkerGlobalScope) {
        info!("gibblox web worker mode: installing open_pipeline handler");

        install_worker(scope);
    }

    fn install_worker(scope: DedicatedWorkerGlobalScope) {
        let state = Rc::new(RefCell::new(WorkerState::default()));
        let scope_for_handler = scope.clone();
        let state_for_handler = Rc::clone(&state);
        let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
            let scope = scope_for_handler.clone();
            let state = Rc::clone(&state_for_handler);
            spawn_local(async move {
                if let Err(err) = handle_worker_message(&scope, &state, event).await {
                    error!(error = %err, "gibblox worker command handling failed");
                }
            });
        });
        scope.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        on_message.forget();

        let on_error = Closure::<dyn FnMut(web_sys::Event)>::new(move |event: web_sys::Event| {
            error!(
                error = %js_value_to_string(event.into()),
                "gibblox worker global transport error"
            );
        });
        scope.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        on_error.forget();

        if let Err(err) = post_ready_response(&scope, WORKER_PROTOCOL_VERSION) {
            error!(error = %err, "failed to post gibblox worker ready response");
        }
    }

    async fn handle_worker_message(
        scope: &DedicatedWorkerGlobalScope,
        state: &Rc<RefCell<WorkerState>>,
        event: MessageEvent,
    ) -> GibbloxResult<()> {
        let data = event.data();
        let id = match prop_u32(&data, "id") {
            Ok(id) => id,
            Err(err) => {
                debug!(error = %err, "ignoring worker message without request id");
                return Ok(());
            }
        };

        let cmd = match prop_string(&data, "cmd") {
            Ok(cmd) => cmd,
            Err(err) => {
                return post_rpc_error_response(scope, id, &err.to_string());
            }
        };

        match cmd.as_str() {
            OPEN_PIPELINE_CMD => {
                if let Err(err) = handle_open_pipeline(scope, state, id, &data).await {
                    post_rpc_error_response(scope, id, &err.to_string())?;
                }
                Ok(())
            }
            SHUTDOWN_CMD => {
                {
                    let mut state = state.borrow_mut();
                    for server in &state.servers {
                        server.stop();
                    }
                    state.servers.clear();
                    state.readers.clear();
                }

                post_rpc_ok_response(scope, id)?;
                scope.close();
                Ok(())
            }
            _ => post_rpc_error_response(
                scope,
                id,
                &format!("unsupported gibblox worker command: {cmd}"),
            ),
        }
    }

    async fn handle_open_pipeline(
        scope: &DedicatedWorkerGlobalScope,
        state: &Rc<RefCell<WorkerState>>,
        id: u32,
        data: &JsValue,
    ) -> GibbloxResult<()> {
        let pipeline_bytes = prop_bytes(data, "pipeline_bin")?;
        let pipeline = decode_pipeline(&pipeline_bytes).map_err(|err| {
            GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!("decode pipeline payload: {err}"),
            )
        })?;
        validate_pipeline(&pipeline).map_err(|err| {
            GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!("validate pipeline payload: {err}"),
            )
        })?;

        let identity = pipeline_identity_string(&pipeline);
        let identity_id = pipeline_identity_id(&pipeline);

        let reader = if let Some(reader) = state.borrow().readers.get(identity.as_str()).cloned() {
            debug!(identity = %identity, identity_id, "reusing resolved pipeline reader");
            reader
        } else {
            let reader = resolve_pipeline_source(&pipeline).await?;
            state
                .borrow_mut()
                .readers
                .insert(identity.clone(), Arc::clone(&reader));
            info!(identity = %identity, identity_id, "resolved new pipeline reader");
            reader
        };

        let size_bytes = size_bytes_for_reader(Arc::clone(&reader)).await?;

        let channel = MessageChannel::new().map_err(js_io)?;
        let server = MessagePortBlockReaderServer::serve(channel.port1(), reader)?;
        let response_port = channel.port2();
        post_open_pipeline_response(scope, id, &identity, size_bytes, &response_port)?;
        state.borrow_mut().servers.push(server);

        Ok(())
    }

    fn resolve_pipeline_source(
        pipeline_source: &PipelineSource,
    ) -> LocalBoxFuture<'_, GibbloxResult<Arc<dyn BlockReader>>> {
        async move {
            match pipeline_source {
                PipelineSource::Http(source) => resolve_http_source(source).await,
                PipelineSource::File(source) => Err(GibbloxError::with_message(
                    GibbloxErrorKind::Unsupported,
                    format!(
                        "pipeline file source is unsupported in web worker runtime: {}",
                        source.file
                    ),
                )),
                PipelineSource::Casync(source) => {
                    resolve_casync_source(source, source_identity(pipeline_source)).await
                }
                PipelineSource::Xz(source) => {
                    let upstream = resolve_pipeline_source(source.xz.as_ref()).await?;
                    let config = XzBlockReaderConfig::default()
                        .with_source_identity(source_identity(source.xz.as_ref()));
                    let reader = XzBlockReader::open_with_block_config(upstream, config).await?;
                    let reader: Arc<dyn BlockReader> = Arc::new(reader);
                    Ok(reader)
                }
                PipelineSource::AndroidSparseImg(source) => {
                    let upstream =
                        resolve_pipeline_source(source.android_sparseimg.as_ref()).await?;
                    let config = AndroidSparseBlockReaderConfig::default()
                        .with_source_identity(source_identity(source.android_sparseimg.as_ref()));
                    let reader =
                        AndroidSparseBlockReader::new_with_config(upstream, config).await?;
                    let reader: Arc<dyn BlockReader> = Arc::new(reader);
                    Ok(reader)
                }
                PipelineSource::Mbr(source) => {
                    let upstream = resolve_pipeline_source(source.mbr.source.as_ref()).await?;
                    let selector = mbr_selector(source)?;
                    let config = MbrBlockReaderConfig::new(selector, upstream.block_size())
                        .with_source_identity(source_identity(source.mbr.source.as_ref()));
                    let reader = MbrBlockReader::open_with_config(upstream, config).await?;
                    let reader: Arc<dyn BlockReader> = Arc::new(reader);
                    Ok(reader)
                }
                PipelineSource::Gpt(source) => {
                    let upstream = resolve_pipeline_source(source.gpt.source.as_ref()).await?;
                    let selector = gpt_selector(source)?;
                    let block_size = upstream.block_size();
                    let reader = GptBlockReader::new(upstream, selector, block_size).await?;
                    let reader: Arc<dyn BlockReader> = Arc::new(reader);
                    Ok(reader)
                }
            }
        }
        .boxed_local()
    }

    async fn resolve_http_source(
        source: &gibblox_pipeline::PipelineSourceHttpSource,
    ) -> GibbloxResult<Arc<dyn BlockReader>> {
        let url = parse_url(source.http.as_str(), "pipeline http source")?;
        let config = HttpBlockReaderConfig::new(url, PIPELINE_BLOCK_SIZE);
        let reader = HttpBlockReader::open(config.clone()).await?;

        let cache = match OpfsCacheOps::open_for_config(&config).await {
            Ok(cache) => cache,
            Err(err) => {
                warn!(error = %err, "failed to open OPFS cache for HTTP source, using uncached reader");
                return Ok(Arc::new(reader));
            }
        };

        match CachedBlockReader::new(reader, cache).await {
            Ok(cached) => Ok(Arc::new(cached)),
            Err(err) => {
                warn!(error = %err, "failed to initialize cached HTTP reader, using uncached reader");
                Ok(Arc::new(HttpBlockReader::open(config).await?))
            }
        }
    }

    async fn resolve_casync_source(
        source: &PipelineSourceCasyncSource,
        identity: String,
    ) -> GibbloxResult<Arc<dyn BlockReader>> {
        let index_url = parse_url(source.casync.index.as_str(), "pipeline casync.index")?;
        let chunk_store_url = match source.casync.chunk_store.as_deref() {
            Some(chunk_store) => parse_url(chunk_store, "pipeline casync.chunk_store")?,
            None => derive_casync_chunk_store_url(&index_url)?,
        };

        let chunk_store_config = WebCasyncChunkStoreConfig::new(chunk_store_url)?;
        let chunk_store = WebCasyncChunkStore::new(chunk_store_config).await?;
        let config = CasyncReaderConfig {
            block_size: PIPELINE_BLOCK_SIZE,
            strict_verify: false,
            identity: Some(identity),
        };

        let reader =
            CasyncBlockReader::open(WebCasyncIndexSource::new(index_url), chunk_store, config)
                .await?;
        Ok(Arc::new(reader))
    }

    fn mbr_selector(
        source: &gibblox_pipeline::PipelineSourceMbrSource,
    ) -> GibbloxResult<MbrPartitionSelector> {
        if let Some(partuuid) = source.mbr.partuuid.as_deref() {
            return Ok(MbrPartitionSelector::part_uuid(partuuid.to_string()));
        }
        if let Some(index) = source.mbr.index {
            return Ok(MbrPartitionSelector::index(index));
        }

        Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "pipeline mbr selector missing",
        ))
    }

    fn gpt_selector(
        source: &gibblox_pipeline::PipelineSourceGptSource,
    ) -> GibbloxResult<GptPartitionSelector> {
        if let Some(partlabel) = source.gpt.partlabel.as_deref() {
            return Ok(GptPartitionSelector::part_label(partlabel.to_string()));
        }
        if let Some(partuuid) = source.gpt.partuuid.as_deref() {
            return Ok(GptPartitionSelector::part_uuid(partuuid.to_string()));
        }
        if let Some(index) = source.gpt.index {
            return Ok(GptPartitionSelector::index(index));
        }

        Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "pipeline gpt selector missing",
        ))
    }

    fn source_identity(source: &PipelineSource) -> String {
        pipeline_identity_string(source)
    }

    fn parse_url(value: &str, context: &str) -> GibbloxResult<Url> {
        let value = value.trim();
        if value.is_empty() {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!("{context} must not be empty"),
            ));
        }

        Url::parse(value).map_err(|err| {
            GibbloxError::with_message(GibbloxErrorKind::InvalidInput, format!("{context}: {err}"))
        })
    }

    fn derive_casync_chunk_store_url(index_url: &Url) -> GibbloxResult<Url> {
        if let Some(segments) = index_url.path_segments() {
            let segments: Vec<&str> = segments.collect();
            if let Some(index_pos) = segments.iter().rposition(|segment| *segment == "indexes") {
                let mut base_segments = segments[..=index_pos].to_vec();
                base_segments[index_pos] = "chunks";

                let mut url = index_url.clone();
                let mut path = String::from("/");
                path.push_str(base_segments.join("/").as_str());
                if !path.ends_with('/') {
                    path.push('/');
                }
                url.set_path(path.as_str());
                url.set_query(None);
                url.set_fragment(None);
                return Ok(url);
            }
        }

        index_url.join("./").map_err(|err| {
            GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!("derive casync chunk store URL from {index_url}: {err}"),
            )
        })
    }

    async fn size_bytes_for_reader(reader: Arc<dyn BlockReader>) -> GibbloxResult<u64> {
        let total_blocks = reader.total_blocks().await?;
        total_blocks
            .checked_mul(reader.block_size() as u64)
            .ok_or_else(|| {
                GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "size overflow")
            })
    }

    fn parse_worker_metadata(data: &JsValue) -> GibbloxResult<WorkerMetadata> {
        Ok(WorkerMetadata {
            protocol_version: prop_u32(data, "protocol_version")?,
        })
    }

    fn try_send_ready_once(
        sender: &Rc<RefCell<Option<oneshot::Sender<GibbloxResult<WorkerMetadata>>>>>,
        value: GibbloxResult<WorkerMetadata>,
    ) -> bool {
        if let Some(sender) = sender.borrow_mut().take() {
            let _ = sender.send(value);
            return true;
        }
        false
    }

    fn reject_all_pending(pending: &PendingMap, err: GibbloxError) {
        if let Ok(mut map) = pending.lock() {
            let senders: Vec<_> = std::mem::take(&mut *map).into_values().collect();
            for sender in senders {
                let _ = sender.send(Err(err.clone()));
            }
        }
    }

    fn response_error(data: &JsValue) -> GibbloxError {
        let message = prop_string(data, "error").unwrap_or_else(|_| "worker rpc error".to_string());
        GibbloxError::with_message(GibbloxErrorKind::Io, message)
    }

    fn post_ready_response(
        scope: &DedicatedWorkerGlobalScope,
        protocol_version: u32,
    ) -> GibbloxResult<()> {
        let response = Object::new();
        set_prop(
            &response,
            "cmd",
            JsValue::from_str(READY_CMD),
            "build worker ready response",
        )?;
        set_prop(
            &response,
            "protocol_version",
            JsValue::from_f64(protocol_version as f64),
            "build worker ready response",
        )?;
        scope.post_message(&response.into()).map_err(js_io)
    }

    fn post_rpc_ok_response(scope: &DedicatedWorkerGlobalScope, id: u32) -> GibbloxResult<()> {
        let response = Object::new();
        set_prop(
            &response,
            "id",
            JsValue::from_f64(id as f64),
            "build rpc response",
        )?;
        set_prop(
            &response,
            "ok",
            JsValue::from_bool(true),
            "build rpc response",
        )?;
        scope.post_message(&response.into()).map_err(js_io)
    }

    fn post_rpc_error_response(
        scope: &DedicatedWorkerGlobalScope,
        id: u32,
        message: &str,
    ) -> GibbloxResult<()> {
        let response = Object::new();
        set_prop(
            &response,
            "id",
            JsValue::from_f64(id as f64),
            "build rpc error response",
        )?;
        set_prop(
            &response,
            "ok",
            JsValue::from_bool(false),
            "build rpc error response",
        )?;
        set_prop(
            &response,
            "error",
            JsValue::from_str(message),
            "build rpc error response",
        )?;
        scope.post_message(&response.into()).map_err(js_io)
    }

    fn post_open_pipeline_response(
        scope: &DedicatedWorkerGlobalScope,
        id: u32,
        identity: &str,
        size_bytes: u64,
        port: &MessagePort,
    ) -> GibbloxResult<()> {
        let response = Object::new();
        set_prop(
            &response,
            "id",
            JsValue::from_f64(id as f64),
            "build open_pipeline response",
        )?;
        set_prop(
            &response,
            "ok",
            JsValue::from_bool(true),
            "build open_pipeline response",
        )?;
        set_prop(
            &response,
            "identity",
            JsValue::from_str(identity),
            "build open_pipeline response",
        )?;
        set_prop(
            &response,
            "size_bytes",
            JsValue::from_str(size_bytes.to_string().as_str()),
            "build open_pipeline response",
        )?;
        set_prop(
            &response,
            "port",
            port.clone().into(),
            "build open_pipeline response",
        )?;

        let transfer = Array::new();
        transfer.push(port.as_ref());
        scope
            .post_message_with_transfer(&response.into(), transfer.as_ref())
            .map_err(js_io)
    }

    fn set_prop(target: &Object, key: &str, value: JsValue, context: &str) -> GibbloxResult<()> {
        Reflect::set(target.as_ref(), &JsValue::from_str(key), &value)
            .map(|_| ())
            .map_err(|err| {
                GibbloxError::with_message(
                    GibbloxErrorKind::Io,
                    format!("{context}: {}", js_value_to_string(err)),
                )
            })
    }

    fn prop_string(target: &JsValue, key: &str) -> GibbloxResult<String> {
        Reflect::get(target, &JsValue::from_str(key))
            .map_err(js_io)?
            .as_string()
            .ok_or_else(|| {
                GibbloxError::with_message(
                    GibbloxErrorKind::InvalidInput,
                    format!("field {key} is missing or not a string"),
                )
            })
    }

    fn prop_u32(target: &JsValue, key: &str) -> GibbloxResult<u32> {
        let value = Reflect::get(target, &JsValue::from_str(key)).map_err(js_io)?;
        parse_u32_value(&value).ok_or_else(|| {
            GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!("field {key} is missing or not a u32"),
            )
        })
    }

    fn prop_u32_opt(target: &JsValue, key: &str) -> Option<u32> {
        Reflect::get(target, &JsValue::from_str(key))
            .ok()
            .and_then(|value| parse_u32_value(&value))
    }

    fn parse_u32_value(value: &JsValue) -> Option<u32> {
        if let Some(number) = value.as_f64() {
            if number.is_finite() && number >= 0.0 && number <= u32::MAX as f64 {
                return Some(number as u32);
            }
        }

        value
            .as_string()
            .and_then(|value| value.parse::<u32>().ok())
    }

    fn prop_bool(target: &JsValue, key: &str) -> GibbloxResult<bool> {
        Reflect::get(target, &JsValue::from_str(key))
            .map_err(js_io)?
            .as_bool()
            .ok_or_else(|| {
                GibbloxError::with_message(
                    GibbloxErrorKind::InvalidInput,
                    format!("field {key} is missing or not a bool"),
                )
            })
    }

    fn prop_u64_string(target: &JsValue, key: &str) -> GibbloxResult<u64> {
        let value = prop_string(target, key)?;
        value.parse::<u64>().map_err(|_| {
            GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!("field {key} has invalid u64 value"),
            )
        })
    }

    fn prop_message_port(target: &JsValue, key: &str) -> GibbloxResult<MessagePort> {
        Reflect::get(target, &JsValue::from_str(key))
            .map_err(js_io)?
            .dyn_into::<MessagePort>()
            .map_err(|_| {
                GibbloxError::with_message(
                    GibbloxErrorKind::InvalidInput,
                    format!("field {key} is missing or not a MessagePort"),
                )
            })
    }

    fn prop_bytes(target: &JsValue, key: &str) -> GibbloxResult<Vec<u8>> {
        let value = Reflect::get(target, &JsValue::from_str(key)).map_err(js_io)?;

        if let Ok(array) = value.clone().dyn_into::<Uint8Array>() {
            return Ok(array.to_vec());
        }
        if let Ok(buffer) = value.dyn_into::<js_sys::ArrayBuffer>() {
            return Ok(Uint8Array::new(&buffer).to_vec());
        }

        Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            format!("field {key} is missing or not a byte array"),
        ))
    }

    fn js_io(err: JsValue) -> GibbloxError {
        GibbloxError::with_message(GibbloxErrorKind::Io, js_value_to_string(err))
    }

    fn js_value_to_string(value: JsValue) -> String {
        js_sys::JSON::stringify(&value)
            .ok()
            .and_then(|s| s.as_string())
            .unwrap_or_else(|| format!("{value:?}"))
    }
}

#[cfg(target_arch = "wasm32")]
pub use wasm::*;
