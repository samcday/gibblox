#[cfg(target_arch = "wasm32")]
mod wasm {
    use gibblox_core::{BlockReader, ReadContext};
    use gibblox_web_worker::GibbloxWebWorker;
    use gloo_timers::future::sleep;
    use js_sys::{Date, Uint8Array};
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::sync::Arc;
    use std::time::Duration;
    use wasm_bindgen::{JsCast, JsValue, closure::Closure, prelude::wasm_bindgen};
    use wasm_bindgen_futures::{JsFuture, spawn_local};
    use web_sys::{
        Document, DragEvent, Event, File, HtmlButtonElement, HtmlElement, HtmlInputElement,
        Response, Worker, WorkerOptions, WorkerType,
    };

    const STATS_REFRESH: Duration = Duration::from_millis(250);
    const DEFAULT_CONCURRENCY: u32 = 8;
    const DEFAULT_REQUEST_KIB: u32 = 1024;
    const MAX_CONCURRENCY: u32 = 64;
    const MAX_REQUEST_KIB: u32 = 65_536;

    thread_local! {
        static APP: RefCell<Option<Rc<App>>> = const { RefCell::new(None) };
    }

    #[derive(Clone, Default)]
    struct PumpCounters {
        bytes: u64,
        requests: u64,
        errors: u64,
        last_error: Option<String>,
    }

    struct PumpSession {
        run_id: u64,
        stop: Rc<Cell<bool>>,
    }

    #[derive(Default)]
    struct RuntimeState {
        worker: Option<Rc<GibbloxWebWorker>>,
        pump: Option<PumpSession>,
        next_run_id: u64,
    }

    #[derive(Clone)]
    struct RunDescriptor {
        source: String,
        pipeline_bytes: usize,
        identity: String,
        target_size_bytes: u64,
        block_size: u32,
        total_blocks: u64,
        open_ms: f64,
        concurrency: u32,
        request_bytes: usize,
    }

    struct PumpSettings {
        concurrency: u32,
        blocks_per_request: u64,
        request_bytes: usize,
    }

    struct App {
        url_input: HtmlInputElement,
        load_url_button: HtmlButtonElement,
        file_picker_button: HtmlButtonElement,
        file_input: HtmlInputElement,
        drop_zone: HtmlElement,
        stop_button: HtmlButtonElement,
        concurrency_input: HtmlInputElement,
        request_kib_input: HtmlInputElement,
        status_output: HtmlElement,
        stats_output: HtmlElement,
        runtime: RefCell<RuntimeState>,
    }

    #[wasm_bindgen]
    pub fn bootstrap_main() -> Result<(), JsValue> {
        console_error_panic_hook::set_once();
        let window = web_sys::window().ok_or_else(|| JsValue::from_str("window is unavailable"))?;
        let document = window
            .document()
            .ok_or_else(|| JsValue::from_str("document is unavailable"))?;

        let app = Rc::new(App::new(&document)?);
        app.install_handlers()?;
        app.set_status_info("Ready. Load a pipeline URL or drop a local pipeline file.");
        APP.with(|slot| {
            *slot.borrow_mut() = Some(app);
        });
        Ok(())
    }

    #[wasm_bindgen]
    pub fn bootstrap_worker() {
        console_error_panic_hook::set_once();
        let _ = gibblox_web_worker::run_if_worker("");
    }

    impl App {
        fn new(document: &Document) -> Result<Self, JsValue> {
            Ok(Self {
                url_input: element_by_id(document, "pipeline-url")?,
                load_url_button: element_by_id(document, "load-url")?,
                file_picker_button: element_by_id(document, "open-file-picker")?,
                file_input: element_by_id(document, "pipeline-file")?,
                drop_zone: element_by_id(document, "drop-zone")?,
                stop_button: element_by_id(document, "stop-pump")?,
                concurrency_input: element_by_id(document, "concurrency")?,
                request_kib_input: element_by_id(document, "request-kib")?,
                status_output: element_by_id(document, "status-line")?,
                stats_output: element_by_id(document, "stats-output")?,
                runtime: RefCell::new(RuntimeState::default()),
            })
        }

        fn install_handlers(self: &Rc<Self>) -> Result<(), JsValue> {
            let app = Rc::clone(self);
            let on_load_url = Closure::<dyn FnMut(Event)>::new(move |event: Event| {
                event.prevent_default();
                let app = Rc::clone(&app);
                spawn_local(async move {
                    app.run_from_url().await;
                });
            });
            self.load_url_button
                .add_event_listener_with_callback("click", on_load_url.as_ref().unchecked_ref())?;
            on_load_url.forget();

            let input = self.file_input.clone();
            let on_open_picker = Closure::<dyn FnMut(Event)>::new(move |event: Event| {
                event.prevent_default();
                input.set_value("");
                input.click();
            });
            self.file_picker_button.add_event_listener_with_callback(
                "click",
                on_open_picker.as_ref().unchecked_ref(),
            )?;
            on_open_picker.forget();

            let app = Rc::clone(self);
            let input = self.file_input.clone();
            let on_file_change = Closure::<dyn FnMut(Event)>::new(move |_event: Event| {
                let Some(list) = input.files() else {
                    return;
                };
                let Some(file) = list.get(0) else {
                    return;
                };
                let app = Rc::clone(&app);
                spawn_local(async move {
                    app.run_from_file(file).await;
                });
            });
            self.file_input.add_event_listener_with_callback(
                "change",
                on_file_change.as_ref().unchecked_ref(),
            )?;
            on_file_change.forget();

            let drop_zone = self.drop_zone.clone();
            let on_drag_over = Closure::<dyn FnMut(Event)>::new(move |event: Event| {
                event.prevent_default();
                let _ = drop_zone.class_list().add_1("is-drop-active");
            });
            self.drop_zone.add_event_listener_with_callback(
                "dragover",
                on_drag_over.as_ref().unchecked_ref(),
            )?;
            on_drag_over.forget();

            let drop_zone = self.drop_zone.clone();
            let on_drag_leave = Closure::<dyn FnMut(Event)>::new(move |event: Event| {
                event.prevent_default();
                let _ = drop_zone.class_list().remove_1("is-drop-active");
            });
            self.drop_zone.add_event_listener_with_callback(
                "dragleave",
                on_drag_leave.as_ref().unchecked_ref(),
            )?;
            on_drag_leave.forget();

            let app = Rc::clone(self);
            let drop_zone = self.drop_zone.clone();
            let on_drop = Closure::<dyn FnMut(Event)>::new(move |event: Event| {
                event.prevent_default();
                let _ = drop_zone.class_list().remove_1("is-drop-active");

                let Ok(drag_event) = event.dyn_into::<DragEvent>() else {
                    return;
                };
                let Some(transfer) = drag_event.data_transfer() else {
                    return;
                };
                let Some(files) = transfer.files() else {
                    return;
                };
                if files.length() == 0 {
                    return;
                }
                let Some(file) = files.get(0) else {
                    return;
                };
                let app = Rc::clone(&app);
                spawn_local(async move {
                    app.run_from_file(file).await;
                });
            });
            self.drop_zone
                .add_event_listener_with_callback("drop", on_drop.as_ref().unchecked_ref())?;
            on_drop.forget();

            let app = Rc::clone(self);
            let on_stop = Closure::<dyn FnMut(Event)>::new(move |event: Event| {
                event.prevent_default();
                app.stop_pump();
                app.set_status_info("Pump stopped.");
            });
            self.stop_button
                .add_event_listener_with_callback("click", on_stop.as_ref().unchecked_ref())?;
            on_stop.forget();

            Ok(())
        }

        async fn run_from_url(self: Rc<Self>) {
            let source = self.url_input.value().trim().to_string();
            if source.is_empty() {
                self.set_status_error("Pipeline URL is empty.");
                return;
            }

            self.set_status_info(format!("Fetching pipeline from {source} ...").as_str());
            match fetch_url_bytes(source.as_str()).await {
                Ok(bytes) => {
                    self.run_pipeline(bytes, format!("url:{source}")).await;
                }
                Err(err) => {
                    self.set_status_error(
                        format!("Failed to fetch pipeline from {source}: {err}").as_str(),
                    );
                }
            }
        }

        async fn run_from_file(self: Rc<Self>, file: File) {
            let file_name = file.name();
            self.set_status_info(format!("Reading pipeline file {file_name} ...").as_str());
            match read_file_bytes(&file).await {
                Ok(bytes) => {
                    self.run_pipeline(bytes, format!("file:{file_name}")).await;
                }
                Err(err) => {
                    self.set_status_error(
                        format!("Failed to read pipeline file {file_name}: {err}").as_str(),
                    );
                }
            }
        }

        async fn run_pipeline(self: Rc<Self>, pipeline_bytes: Vec<u8>, source: String) {
            self.stop_pump();

            self.set_status_info("Starting worker ...");
            let worker = match self.ensure_worker().await {
                Ok(worker) => worker,
                Err(err) => {
                    self.set_status_error(format!("Worker startup failed: {err}").as_str());
                    return;
                }
            };

            self.set_status_info("Opening pipeline in worker ...");
            let open_started = Date::now();
            let open_result = match worker.open_pipeline(&pipeline_bytes).await {
                Ok(result) => result,
                Err(err) => {
                    self.set_status_error(format!("open_pipeline failed: {err}").as_str());
                    return;
                }
            };

            let reader = Arc::new(open_result.reader);
            let block_size = reader.block_size();
            let total_blocks = match reader.total_blocks().await {
                Ok(total_blocks) => total_blocks,
                Err(err) => {
                    self.set_status_error(
                        format!("Failed to query total_blocks from worker reader: {err}").as_str(),
                    );
                    return;
                }
            };

            if total_blocks == 0 {
                self.set_status_error("Resolved pipeline reader has zero blocks.");
                return;
            }

            let settings = self.pump_settings(block_size);
            let descriptor = RunDescriptor {
                source,
                pipeline_bytes: pipeline_bytes.len(),
                identity: open_result.identity,
                target_size_bytes: open_result.size_bytes,
                block_size,
                total_blocks,
                open_ms: Date::now() - open_started,
                concurrency: settings.concurrency,
                request_bytes: settings.request_bytes,
            };

            let run_id = {
                let mut runtime = self.runtime.borrow_mut();
                runtime.next_run_id = runtime.next_run_id.saturating_add(1);
                let run_id = runtime.next_run_id;
                runtime.pump = Some(PumpSession {
                    run_id,
                    stop: Rc::new(Cell::new(false)),
                });
                run_id
            };

            let stop = {
                let runtime = self.runtime.borrow();
                runtime
                    .pump
                    .as_ref()
                    .map(|pump| Rc::clone(&pump.stop))
                    .expect("pump session must exist after creation")
            };

            self.stop_button.set_disabled(false);
            self.set_status_info("Pipeline open. Pumping reads ...");

            let counters = Rc::new(RefCell::new(PumpCounters::default()));
            let started = Date::now();
            self.render_stats(
                &descriptor,
                &counters.borrow(),
                started,
                Some("Pump started"),
            );

            for lane in 0..settings.concurrency {
                let reader = Arc::clone(&reader);
                let counters = Rc::clone(&counters);
                let stop = Rc::clone(&stop);
                let blocks_per_request = settings.blocks_per_request;
                let request_bytes = settings.request_bytes;
                let total_blocks = descriptor.total_blocks;
                let stride = blocks_per_request.saturating_mul(settings.concurrency as u64);

                spawn_local(async move {
                    let mut lba = (lane as u64)
                        .saturating_mul(blocks_per_request)
                        .wrapping_rem(total_blocks);
                    let mut buf = vec![0u8; request_bytes];

                    while !stop.get() {
                        match reader
                            .read_blocks(lba, &mut buf, ReadContext::FOREGROUND)
                            .await
                        {
                            Ok(read) => {
                                if read == 0 {
                                    let mut counters = counters.borrow_mut();
                                    counters.errors = counters.errors.saturating_add(1);
                                    if counters.last_error.is_none() {
                                        counters.last_error =
                                            Some("read_blocks returned zero bytes".to_string());
                                    }
                                    stop.set(true);
                                    continue;
                                }
                                let mut counters = counters.borrow_mut();
                                counters.bytes = counters.bytes.saturating_add(read as u64);
                                counters.requests = counters.requests.saturating_add(1);
                            }
                            Err(err) => {
                                let mut counters = counters.borrow_mut();
                                counters.errors = counters.errors.saturating_add(1);
                                if counters.last_error.is_none() {
                                    counters.last_error = Some(err.to_string());
                                }
                                stop.set(true);
                            }
                        }

                        lba = lba.saturating_add(stride).wrapping_rem(total_blocks);
                    }
                });
            }

            let app = Rc::clone(&self);
            spawn_local(async move {
                loop {
                    if app.active_run_id() != Some(run_id) {
                        return;
                    }

                    let snapshot = counters.borrow().clone();
                    app.render_stats(&descriptor, &snapshot, started, None);

                    if stop.get() {
                        break;
                    }

                    sleep(STATS_REFRESH).await;
                }

                if app.active_run_id() != Some(run_id) {
                    return;
                }

                let snapshot = counters.borrow().clone();
                app.finish_run(run_id, &descriptor, &snapshot, started);
            });
        }

        async fn ensure_worker(&self) -> Result<Rc<GibbloxWebWorker>, String> {
            if let Some(existing) = self.runtime.borrow().worker.clone() {
                return Ok(existing);
            }

            let options = WorkerOptions::new();
            options.set_type(WorkerType::Module);
            options.set_name("gibblox-pipeline-pump");

            let worker =
                Worker::new_with_options("worker.js", &options).map_err(js_value_to_string)?;
            let wrapped = GibbloxWebWorker::new(worker)
                .await
                .map_err(|err| err.to_string())?;
            let wrapped = Rc::new(wrapped);

            self.runtime.borrow_mut().worker = Some(Rc::clone(&wrapped));
            Ok(wrapped)
        }

        fn pump_settings(&self, block_size: u32) -> PumpSettings {
            let concurrency = parse_u32_input(
                &self.concurrency_input,
                DEFAULT_CONCURRENCY,
                1,
                MAX_CONCURRENCY,
            );
            self.concurrency_input
                .set_value(concurrency.to_string().as_str());

            let request_kib = parse_u32_input(
                &self.request_kib_input,
                DEFAULT_REQUEST_KIB,
                1,
                MAX_REQUEST_KIB,
            );

            let requested_bytes = u64::from(request_kib).saturating_mul(1024);
            let block_size = u64::from(block_size.max(1));

            let max_blocks = (u32::MAX as u64 / block_size).max(1);
            let blocks_per_request = (requested_bytes / block_size).max(1).min(max_blocks);
            let request_bytes_u64 = blocks_per_request.saturating_mul(block_size);
            let request_bytes = usize::try_from(request_bytes_u64).unwrap_or(u32::MAX as usize);

            let aligned_kib = u32::try_from((request_bytes_u64 / 1024).max(1)).unwrap_or(u32::MAX);
            self.request_kib_input
                .set_value(aligned_kib.to_string().as_str());

            PumpSettings {
                concurrency,
                blocks_per_request,
                request_bytes,
            }
        }

        fn stop_pump(&self) {
            let previous = self.runtime.borrow_mut().pump.take();
            if let Some(session) = previous {
                session.stop.set(true);
            }
            self.stop_button.set_disabled(true);
        }

        fn active_run_id(&self) -> Option<u64> {
            self.runtime.borrow().pump.as_ref().map(|pump| pump.run_id)
        }

        fn finish_run(
            &self,
            run_id: u64,
            descriptor: &RunDescriptor,
            counters: &PumpCounters,
            started: f64,
        ) {
            if self.active_run_id() != Some(run_id) {
                return;
            }

            self.render_stats(descriptor, counters, started, Some("Pump complete"));
            self.stop_pump();

            if let Some(message) = counters.last_error.as_deref() {
                self.set_status_error(format!("Pump stopped on error: {message}").as_str());
            } else {
                self.set_status_info("Pump finished.");
            }
        }

        fn set_status_info(&self, message: &str) {
            self.status_output.set_class_name("status-line status-info");
            self.status_output.set_text_content(Some(message));
        }

        fn set_status_error(&self, message: &str) {
            self.status_output
                .set_class_name("status-line status-error");
            self.status_output.set_text_content(Some(message));
        }

        fn render_stats(
            &self,
            descriptor: &RunDescriptor,
            counters: &PumpCounters,
            started: f64,
            phase: Option<&str>,
        ) {
            let elapsed = ((Date::now() - started) / 1000.0).max(0.0);
            let throughput = if elapsed > 0.0 {
                counters.bytes as f64 / elapsed
            } else {
                0.0
            };
            let req_rate = if elapsed > 0.0 {
                counters.requests as f64 / elapsed
            } else {
                0.0
            };

            let output = format!(
                "phase: {}\nsource: {}\npipeline_bytes: {} ({})\nreader_identity: {}\ntarget_size: {} ({})\nblock_size: {}\ntotal_blocks: {}\nopen_latency_ms: {:.2}\n\nconcurrency: {}\nrequest_bytes: {} ({})\n\nelapsed_s: {:.2}\nbytes_read: {} ({})\nrequests: {}\nerrors: {}\nthroughput_mib_s: {:.2}\nrequest_rate_s: {:.2}",
                phase.unwrap_or("Pumping"),
                descriptor.source,
                descriptor.pipeline_bytes,
                format_bytes(descriptor.pipeline_bytes as u64),
                descriptor.identity,
                descriptor.target_size_bytes,
                format_bytes(descriptor.target_size_bytes),
                descriptor.block_size,
                descriptor.total_blocks,
                descriptor.open_ms,
                descriptor.concurrency,
                descriptor.request_bytes,
                format_bytes(descriptor.request_bytes as u64),
                elapsed,
                counters.bytes,
                format_bytes(counters.bytes),
                counters.requests,
                counters.errors,
                throughput / (1024.0 * 1024.0),
                req_rate,
            );

            self.stats_output.set_text_content(Some(output.as_str()));
        }
    }

    async fn fetch_url_bytes(url: &str) -> Result<Vec<u8>, String> {
        let window = web_sys::window().ok_or_else(|| "window is unavailable".to_string())?;
        let response_value = JsFuture::from(window.fetch_with_str(url))
            .await
            .map_err(js_value_to_string)?;
        let response = response_value
            .dyn_into::<Response>()
            .map_err(js_value_to_string)?;

        if !response.ok() {
            return Err(format!(
                "HTTP {} {} while fetching {}",
                response.status(),
                response.status_text(),
                url
            ));
        }

        let promise = response.array_buffer().map_err(js_value_to_string)?;
        let buffer = JsFuture::from(promise).await.map_err(js_value_to_string)?;
        Ok(Uint8Array::new(&buffer).to_vec())
    }

    async fn read_file_bytes(file: &File) -> Result<Vec<u8>, String> {
        let buffer = JsFuture::from(file.array_buffer())
            .await
            .map_err(js_value_to_string)?;
        Ok(Uint8Array::new(&buffer).to_vec())
    }

    fn element_by_id<T: JsCast>(document: &Document, id: &str) -> Result<T, JsValue> {
        let element = document
            .get_element_by_id(id)
            .ok_or_else(|| JsValue::from_str(format!("missing required element #{id}").as_str()))?;
        element
            .dyn_into::<T>()
            .map_err(|_| JsValue::from_str(format!("element #{id} has wrong type").as_str()))
    }

    fn parse_u32_input(input: &HtmlInputElement, fallback: u32, min: u32, max: u32) -> u32 {
        input
            .value()
            .trim()
            .parse::<u32>()
            .ok()
            .unwrap_or(fallback)
            .clamp(min, max)
    }

    fn format_bytes(bytes: u64) -> String {
        const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
        let mut value = bytes as f64;
        let mut idx = 0usize;
        while value >= 1024.0 && idx < UNITS.len() - 1 {
            value /= 1024.0;
            idx += 1;
        }
        format!("{value:.2} {}", UNITS[idx])
    }

    fn js_value_to_string(value: JsValue) -> String {
        js_sys::JSON::stringify(&value)
            .ok()
            .and_then(|s| s.as_string())
            .filter(|s| !s.is_empty() && s != "null")
            .or_else(|| value.as_string())
            .unwrap_or_else(|| format!("{value:?}"))
    }
}

#[cfg(target_arch = "wasm32")]
pub use wasm::*;
