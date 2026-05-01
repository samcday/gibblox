#[cfg(target_arch = "wasm32")]
mod wasm {
    use futures_channel::oneshot;
    use futures_util::{FutureExt, future::select};
    use gibblox_blockreader_messageport::{
        MessagePortBlockReaderClient, MessagePortBlockReaderServer,
    };
    use gibblox_core::{
        BlockIdentityHasher32, BlockReader, GibbloxError, GibbloxErrorKind, GibbloxResult,
    };
    use gibblox_pipeline::{
        OpenWebPipelineOptions, PipelineCachePolicy, decode_pipeline, decode_pipeline_hints,
        encode_pipeline_hints, open_pipeline_web, pipeline_identity_id, pipeline_identity_string,
        validate_pipeline,
    };
    use gloo_timers::future::sleep;
    use js_sys::{Array, Object, Reflect, Uint8Array};
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::hash::Hasher;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tracing::{debug, error, info};
    use wasm_bindgen::{JsCast, JsValue, closure::Closure};
    use wasm_bindgen_futures::spawn_local;
    use web_sys::{DedicatedWorkerGlobalScope, MessageChannel, MessageEvent, MessagePort, Worker};

    const READY_TIMEOUT: Duration = Duration::from_secs(5);
    const WORKER_PROTOCOL_VERSION: u32 = 1;
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

    #[derive(Clone, Debug, Default)]
    pub struct OpenPipelineRequestOptions {
        pub image_block_size: Option<u32>,
        pub cache_policy: Option<PipelineCachePolicy>,
        pub pipeline_hints_bin: Option<Vec<u8>>,
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
            self.open_pipeline_with_options(pipeline_bytes, &OpenPipelineRequestOptions::default())
                .await
        }

        pub async fn open_pipeline_with_options(
            &self,
            pipeline_bytes: &[u8],
            options: &OpenPipelineRequestOptions,
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

            if options.image_block_size.is_some()
                || options.cache_policy.is_some()
                || options.pipeline_hints_bin.is_some()
            {
                let request_options = Object::new();
                if let Some(image_block_size) = options.image_block_size {
                    set_prop(
                        &request_options,
                        "image_block_size",
                        JsValue::from_f64(image_block_size as f64),
                        "build open_pipeline request",
                    )?;
                }
                if let Some(cache_policy) = options.cache_policy {
                    set_prop(
                        &request_options,
                        "cache_policy",
                        JsValue::from_str(cache_policy.as_str()),
                        "build open_pipeline request",
                    )?;
                }
                if let Some(pipeline_hints_bin) = options.pipeline_hints_bin.as_deref() {
                    let len = u32::try_from(pipeline_hints_bin.len()).map_err(|_| {
                        GibbloxError::with_message(
                            GibbloxErrorKind::OutOfRange,
                            "pipeline hints payload exceeds browser transfer limits",
                        )
                    })?;
                    let bytes = Uint8Array::new_with_length(len);
                    bytes.copy_from(pipeline_hints_bin);
                    set_prop(
                        &request_options,
                        "pipeline_hints_bin",
                        bytes.into(),
                        "build open_pipeline request",
                    )?;
                }
                set_prop(
                    &request,
                    "options",
                    request_options.into(),
                    "build open_pipeline request",
                )?;
            }

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

        let open_options = parse_open_pipeline_options(data)?;

        let identity = pipeline_identity_string(&pipeline);
        let identity_id = pipeline_identity_id(&pipeline);
        let endpoint_key = pipeline_endpoint_key(identity.as_str(), &open_options);
        let cache_policy = effective_cache_policy(&open_options);
        let image_block_size = open_options.image_block_size;

        let reader =
            if let Some(reader) = state.borrow().readers.get(endpoint_key.as_str()).cloned() {
                debug!(
                    identity = %identity,
                    identity_id,
                    endpoint_key = %endpoint_key,
                    cache_policy = %cache_policy,
                    image_block_size,
                    "reusing resolved pipeline reader"
                );
                reader
            } else {
                let reader = open_pipeline_web(&pipeline, &open_options).await?;
                state
                    .borrow_mut()
                    .readers
                    .insert(endpoint_key.clone(), Arc::clone(&reader));
                info!(
                    identity = %identity,
                    identity_id,
                    endpoint_key = %endpoint_key,
                    cache_policy = %cache_policy,
                    image_block_size,
                    "resolved new pipeline reader"
                );
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

    async fn size_bytes_for_reader(reader: Arc<dyn BlockReader>) -> GibbloxResult<u64> {
        let total_blocks = reader.total_blocks().await?;
        total_blocks
            .checked_mul(reader.block_size() as u64)
            .ok_or_else(|| {
                GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "size overflow")
            })
    }

    fn parse_open_pipeline_options(data: &JsValue) -> GibbloxResult<OpenWebPipelineOptions> {
        let mut options = OpenWebPipelineOptions::default();
        let raw = Reflect::get(data, &JsValue::from_str("options")).map_err(js_io)?;
        if raw.is_undefined() || raw.is_null() {
            return Ok(options);
        }
        if !raw.is_object() {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                "field options is not an object",
            ));
        }

        if let Some(image_block_size) = prop_u32_opt(&raw, "image_block_size") {
            options.image_block_size = image_block_size;
        }
        if let Some(cache_http_sources) = prop_bool_opt(&raw, "cache_http_sources") {
            options.cache_http_sources = cache_http_sources;
        }

        let raw_hints =
            Reflect::get(&raw, &JsValue::from_str("pipeline_hints_bin")).map_err(js_io)?;
        if !raw_hints.is_undefined() && !raw_hints.is_null() {
            let hint_bytes = prop_bytes(&raw, "pipeline_hints_bin")?;
            let hints = decode_pipeline_hints(&hint_bytes).map_err(|err| {
                GibbloxError::with_message(
                    GibbloxErrorKind::InvalidInput,
                    format!("decode pipeline hints payload: {err}"),
                )
            })?;
            options.pipeline_hints = Some(hints);
        }

        let raw_policy = Reflect::get(&raw, &JsValue::from_str("cache_policy")).map_err(js_io)?;
        if !raw_policy.is_undefined() && !raw_policy.is_null() {
            let policy = raw_policy.as_string().ok_or_else(|| {
                GibbloxError::with_message(
                    GibbloxErrorKind::InvalidInput,
                    "field options.cache_policy is not a string",
                )
            })?;
            let policy = match parse_cache_policy(policy.as_str()) {
                Some(policy) => policy,
                None => {
                    return Err(GibbloxError::with_message(
                        GibbloxErrorKind::InvalidInput,
                        format!("field options.cache_policy has invalid value: {policy}"),
                    ));
                }
            };
            options.cache_policy = Some(policy);
        }

        Ok(options)
    }

    fn parse_cache_policy(value: &str) -> Option<PipelineCachePolicy> {
        match value.to_ascii_lowercase().as_str() {
            "none" => Some(PipelineCachePolicy::None),
            "head" => Some(PipelineCachePolicy::Head),
            "tail" => Some(PipelineCachePolicy::Tail),
            _ => None,
        }
    }

    fn effective_cache_policy(options: &OpenWebPipelineOptions) -> PipelineCachePolicy {
        if let Some(policy) = options.cache_policy {
            return policy;
        }

        if options.cache_http_sources {
            PipelineCachePolicy::Head
        } else {
            PipelineCachePolicy::None
        }
    }

    fn pipeline_endpoint_key(identity: &str, options: &OpenWebPipelineOptions) -> String {
        format!(
            "{identity}:block_size={}:cache_policy={}:{}",
            options.image_block_size,
            effective_cache_policy(options),
            pipeline_hints_cache_key(options)
        )
    }

    fn pipeline_hints_cache_key(options: &OpenWebPipelineOptions) -> String {
        let Some(hints) = options.pipeline_hints.as_ref() else {
            return "hints=none".to_string();
        };
        let encoded = match encode_pipeline_hints(hints) {
            Ok(encoded) => encoded,
            Err(err) => return format!("hints=invalid:{err}"),
        };
        let mut hasher = BlockIdentityHasher32::new();
        hasher.write(encoded.as_slice());
        format!("hints={}:{}", encoded.len(), hasher.finish() as u32)
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

    fn prop_bool_opt(target: &JsValue, key: &str) -> Option<bool> {
        Reflect::get(target, &JsValue::from_str(key))
            .ok()
            .and_then(|value| value.as_bool())
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
