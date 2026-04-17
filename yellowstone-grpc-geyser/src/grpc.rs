use {
    crate::{
        config::{ConfigGrpc, ConfigTokio},
        metrics::{self, DebugClientMessage},
        version::GrpcVersionInfo,
    },
    anyhow::Context,
    log::{error, info},
    solana_sdk::clock::MAX_RECENT_BLOCKHASHES,
    std::{
        collections::{BTreeMap, HashMap},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    },
    tokio::{
        fs,
        runtime::Builder,
        sync::{broadcast, mpsc, Mutex, Notify, RwLock, Semaphore},
        task::spawn_blocking,
        time::{sleep, Duration},
    },
    tokio_stream::wrappers::ReceiverStream,
    tonic::{
        service::interceptor::interceptor,
        transport::{
            server::{Server, TcpIncoming},
            Identity, ServerTlsConfig,
        },
        Request, Response, Result as TonicResult, Status, Streaming,
    },
    tonic_health::server::health_reporter,
    yellowstone_grpc_proto::{
        plugin::{
            filter::{
                limits::FilterLimits,
                message::{FilteredUpdate, FilteredUpdateOneof},
                name::FilterNames,
                Filter,
            },
            message::{CommitmentLevel, Message, MessageBlockMeta, SlotStatus},
            proto::geyser_server::{Geyser, GeyserServer},
        },
        prelude::{
            CommitmentLevel as CommitmentLevelProto, GetBlockHeightRequest, GetBlockHeightResponse,
            GetLatestBlockhashRequest, GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse,
            GetVersionRequest, GetVersionResponse, IsBlockhashValidRequest,
            IsBlockhashValidResponse, PingRequest, PongResponse, SubscribeRequest,
        },
    },
};

#[derive(Debug)]
struct BlockhashStatus {
    slot: u64,
    processed: bool,
    confirmed: bool,
    finalized: bool,
}

impl BlockhashStatus {
    const fn new(slot: u64) -> Self {
        Self {
            slot,
            processed: false,
            confirmed: false,
            finalized: false,
        }
    }
}

#[derive(Debug, Default)]
struct BlockMetaStorageInner {
    blocks: HashMap<u64, Arc<MessageBlockMeta>>,
    blockhashes: HashMap<String, BlockhashStatus>,
    processed: Option<u64>,
    confirmed: Option<u64>,
    finalized: Option<u64>,
}

#[derive(Debug)]
struct BlockMetaStorage {
    read_sem: Semaphore,
    inner: Arc<RwLock<BlockMetaStorageInner>>,
}

impl BlockMetaStorage {
    fn new(unary_concurrency_limit: usize) -> (Self, mpsc::UnboundedSender<Message>) {
        let inner = Arc::new(RwLock::new(BlockMetaStorageInner::default()));
        let (tx, mut rx) = mpsc::unbounded_channel();

        let storage = Arc::clone(&inner);
        tokio::spawn(async move {
            const KEEP_SLOTS: u64 = 3;

            while let Some(message) = rx.recv().await {
                let mut storage = storage.write().await;
                match message {
                    Message::Slot(msg) => {
                        match msg.status {
                            SlotStatus::Processed => {
                                storage.processed.replace(msg.slot);
                            }
                            SlotStatus::Confirmed => {
                                storage.confirmed.replace(msg.slot);
                            }
                            SlotStatus::Finalized => {
                                storage.finalized.replace(msg.slot);
                            }
                            _ => {}
                        }

                        if let Some(blockhash) = storage
                            .blocks
                            .get(&msg.slot)
                            .map(|block| block.blockhash.clone())
                        {
                            let entry = storage
                                .blockhashes
                                .entry(blockhash)
                                .or_insert_with(|| BlockhashStatus::new(msg.slot));

                            match msg.status {
                                SlotStatus::Processed => {
                                    entry.processed = true;
                                }
                                SlotStatus::Confirmed => {
                                    entry.confirmed = true;
                                }
                                SlotStatus::Finalized => {
                                    entry.finalized = true;
                                }
                                _ => {}
                            }
                        }

                        if msg.status == SlotStatus::Finalized {
                            if let Some(keep_slot) = msg.slot.checked_sub(KEEP_SLOTS) {
                                storage.blocks.retain(|slot, _block| *slot >= keep_slot);
                            }

                            if let Some(keep_slot) =
                                msg.slot.checked_sub(MAX_RECENT_BLOCKHASHES as u64 + 32)
                            {
                                storage
                                    .blockhashes
                                    .retain(|_blockhash, status| status.slot >= keep_slot);
                            }
                        }
                    }
                    Message::BlockMeta(msg) => {
                        storage.blocks.insert(msg.slot, msg);
                    }
                    msg => {
                        error!("invalid message in BlockMetaStorage: {msg:?}");
                    }
                }
            }
        });

        (
            Self {
                read_sem: Semaphore::new(unary_concurrency_limit),
                inner,
            },
            tx,
        )
    }

    fn parse_commitment(commitment: Option<i32>) -> Result<CommitmentLevel, Status> {
        let commitment = commitment.unwrap_or(CommitmentLevelProto::Processed as i32);
        CommitmentLevelProto::try_from(commitment)
            .map(Into::into)
            .map_err(|_error| {
                let msg = format!("failed to create CommitmentLevel from {commitment:?}");
                Status::unknown(msg)
            })
    }

    async fn get_block<F, T>(
        &self,
        handler: F,
        commitment: Option<i32>,
    ) -> Result<Response<T>, Status>
    where
        F: FnOnce(&MessageBlockMeta) -> Option<T>,
    {
        let commitment = Self::parse_commitment(commitment)?;
        let _permit = self.read_sem.acquire().await;
        let storage = self.inner.read().await;

        let slot = match commitment {
            CommitmentLevel::Processed => storage.processed,
            CommitmentLevel::Confirmed => storage.confirmed,
            CommitmentLevel::Finalized => storage.finalized,
        };

        match slot.and_then(|slot| storage.blocks.get(&slot)) {
            Some(block) => match handler(block) {
                Some(resp) => Ok(Response::new(resp)),
                None => Err(Status::internal("failed to build response")),
            },
            None => Err(Status::internal("block is not available yet")),
        }
    }

    async fn is_blockhash_valid(
        &self,
        blockhash: &str,
        commitment: Option<i32>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        let commitment = Self::parse_commitment(commitment)?;
        let _permit = self.read_sem.acquire().await;
        let storage = self.inner.read().await;

        if storage.blockhashes.len() < MAX_RECENT_BLOCKHASHES + 32 {
            return Err(Status::internal("startup"));
        }

        let slot = match commitment {
            CommitmentLevel::Processed => storage.processed,
            CommitmentLevel::Confirmed => storage.confirmed,
            CommitmentLevel::Finalized => storage.finalized,
        }
        .ok_or_else(|| Status::internal("startup"))?;

        let valid = storage
            .blockhashes
            .get(blockhash)
            .map(|status| match commitment {
                CommitmentLevel::Processed => status.processed,
                CommitmentLevel::Confirmed => status.confirmed,
                CommitmentLevel::Finalized => status.finalized,
            })
            .unwrap_or(false);

        Ok(Response::new(IsBlockhashValidResponse { valid, slot }))
    }
}

#[derive(Debug, Default)]
struct ReplayStorage {
    slots: BTreeMap<u64, Vec<(u64, Message)>>,
}

impl ReplayStorage {
    fn push(&mut self, msgid: u64, message: Message) {
        self.slots.entry(message.get_slot()).or_default().push((msgid, message));
    }

    fn trim(&mut self, finalized_slot: u64, keep_slots: u64) {
        let keep_slot = finalized_slot.checked_sub(keep_slots).unwrap_or_default();
        self.slots.retain(|slot, _| *slot >= keep_slot);
    }

    fn snapshot_from(&self, from_slot: u64) -> Result<(Vec<(u64, Message)>, Option<u64>), u64> {
        match self.slots.first_key_value() {
            Some((slot, _)) if from_slot < *slot => Err(*slot),
            _ => {
                let mut messages = Vec::new();
                for (_slot, slot_messages) in self.slots.range(from_slot..) {
                    messages.extend(slot_messages.iter().cloned());
                }
                let last_msgid = messages.last().map(|(msgid, _)| *msgid);
                Ok((messages, last_msgid))
            }
        }
    }
}

#[derive(Debug, Default)]
struct MessageId {
    id: u64,
}

impl MessageId {
    const fn next(&mut self) -> u64 {
        self.id = self.id.checked_add(1).expect("message id overflow");
        self.id
    }
}

type BroadcastedMessage = (CommitmentLevel, Arc<Vec<(u64, Message)>>);

#[derive(Debug)]
pub struct GrpcService {
    config_snapshot_client_channel_capacity: usize,
    config_channel_capacity: usize,
    config_filter_limits: Arc<FilterLimits>,
    blocks_meta: Option<BlockMetaStorage>,
    subscribe_id: AtomicUsize,
    snapshot_rx: Mutex<Option<crossbeam_channel::Receiver<Box<Message>>>>,
    broadcast_tx: broadcast::Sender<BroadcastedMessage>,
    replay: Arc<RwLock<ReplayStorage>>,
    debug_clients_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
    filter_names: Arc<Mutex<FilterNames>>,
}

impl GrpcService {
    #[allow(clippy::type_complexity)]
    pub async fn create(
        config_tokio: ConfigTokio,
        config: ConfigGrpc,
        debug_clients_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
        is_reload: bool,
    ) -> anyhow::Result<(
        Option<crossbeam_channel::Sender<Box<Message>>>,
        mpsc::UnboundedSender<Message>,
        Arc<Notify>,
    )> {
        // Bind service address
        let incoming = TcpIncoming::new(
            config.address,
            true,                          // tcp_nodelay
            Some(Duration::from_secs(20)), // tcp_keepalive
        )
        .map_err(|error| anyhow::anyhow!(error))?;

        // Snapshot channel
        let (snapshot_tx, snapshot_rx) = match config.snapshot_plugin_channel_capacity {
            Some(cap) if !is_reload => {
                let (tx, rx) = crossbeam_channel::bounded(cap);
                (Some(tx), Some(rx))
            }
            _ => (None, None),
        };

        // Blocks meta storage
        let (blocks_meta, blocks_meta_tx) = if config.unary_disabled {
            (None, None)
        } else {
            let (blocks_meta, blocks_meta_tx) =
                BlockMetaStorage::new(config.unary_concurrency_limit);
            (Some(blocks_meta), Some(blocks_meta_tx))
        };

        // Messages to clients
        let (broadcast_tx, _) = broadcast::channel(config.channel_capacity);
        let replay = Arc::new(RwLock::new(ReplayStorage::default()));

        // gRPC server builder with optional TLS
        let mut server_builder = Server::builder();
        if let Some(tls_config) = &config.tls_config {
            let (cert, key) = tokio::try_join!(
                fs::read(&tls_config.cert_path),
                fs::read(&tls_config.key_path)
            )
            .context("failed to load tls_config files")?;
            server_builder = server_builder
                .tls_config(ServerTlsConfig::new().identity(Identity::from_pem(cert, key)))
                .context("failed to apply tls_config")?;
        }
        if let Some(enabled) = config.server_http2_adaptive_window {
            server_builder = server_builder.http2_adaptive_window(Some(enabled));
        }
        if let Some(http2_keepalive_interval) = config.server_http2_keepalive_interval {
            server_builder =
                server_builder.http2_keepalive_interval(Some(http2_keepalive_interval));
        }
        if let Some(http2_keepalive_timeout) = config.server_http2_keepalive_timeout {
            server_builder = server_builder.http2_keepalive_timeout(Some(http2_keepalive_timeout));
        }
        if let Some(sz) = config.server_initial_connection_window_size {
            server_builder = server_builder.initial_connection_window_size(sz);
        }
        if let Some(sz) = config.server_initial_stream_window_size {
            server_builder = server_builder.initial_stream_window_size(sz);
        }

        let filter_names = Arc::new(Mutex::new(FilterNames::new(
            config.filter_name_size_limit,
            config.filter_names_size_limit,
            config.filter_names_cleanup_interval,
        )));

        // Create Server
        let max_decoding_message_size = config.max_decoding_message_size;
        let mut service = GeyserServer::new(Self {
            config_snapshot_client_channel_capacity: config.snapshot_client_channel_capacity,
            config_channel_capacity: config.channel_capacity,
            config_filter_limits: Arc::new(config.filter_limits),
            blocks_meta,
            subscribe_id: AtomicUsize::new(0),
            snapshot_rx: Mutex::new(snapshot_rx),
            broadcast_tx: broadcast_tx.clone(),
            replay: Arc::clone(&replay),
            debug_clients_tx,
            filter_names,
        })
        .max_decoding_message_size(max_decoding_message_size);
        for encoding in config.compression.accept {
            service = service.accept_compressed(encoding);
        }
        for encoding in config.compression.send {
            service = service.send_compressed(encoding);
        }

        // Run geyser message loop
        let (messages_tx, messages_rx) = mpsc::unbounded_channel();
        spawn_blocking(move || {
            let mut builder = Builder::new_multi_thread();
            if let Some(worker_threads) = config_tokio.worker_threads {
                builder.worker_threads(worker_threads);
            }
            builder
                .thread_name_fn(crate::get_thread_name)
                .enable_all()
                .build()
                .expect("Failed to create a new runtime for geyser loop")
                .block_on(Self::geyser_loop(
                    messages_rx,
                    blocks_meta_tx,
                    broadcast_tx,
                    replay,
                    config.replay_stored_slots,
                ));
        });

        // Run Server
        let shutdown = Arc::new(Notify::new());
        let shutdown_grpc = Arc::clone(&shutdown);
        tokio::spawn(async move {
            // gRPC Health check service
            let (mut health_reporter, health_service) = health_reporter();
            health_reporter.set_serving::<GeyserServer<Self>>().await;

            server_builder
                .layer(interceptor(move |request: Request<()>| {
                    if let Some(x_token) = &config.x_token {
                        match request.metadata().get("x-token") {
                            Some(token) if x_token == token => Ok(request),
                            _ => Err(Status::unauthenticated("No valid auth token")),
                        }
                    } else {
                        Ok(request)
                    }
                }))
                .add_service(health_service)
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, shutdown_grpc.notified())
                .await
        });

        Ok((snapshot_tx, messages_tx, shutdown))
    }

    async fn geyser_loop(
        mut messages_rx: mpsc::UnboundedReceiver<Message>,
        blocks_meta_tx: Option<mpsc::UnboundedSender<Message>>,
        broadcast_tx: broadcast::Sender<BroadcastedMessage>,
        replay: Arc<RwLock<ReplayStorage>>,
        replay_stored_slots: u64,
    ) {
        let mut msgid_gen = MessageId::default();
        let keep_slots = 10_u64.saturating_add(replay_stored_slots);

        loop {
            tokio::select! {
                Some(message) = messages_rx.recv() => {
                    metrics::message_queue_size_dec();
                    let msgid = msgid_gen.next();

                    if let Message::Slot(slot_message) = &message {
                        metrics::update_slot_plugin_status(slot_message.status, slot_message.slot);
                    }

                    if let Some(blocks_meta_tx) = &blocks_meta_tx {
                        if matches!(&message, Message::Slot(_) | Message::BlockMeta(_)) {
                            let _ = blocks_meta_tx.send(message.clone());
                        }
                    }

                    {
                        let mut replay = replay.write().await;
                        replay.push(msgid, message.clone());
                        if let Message::Slot(slot_message) = &message {
                            if slot_message.status == SlotStatus::Finalized {
                                replay.trim(slot_message.slot, keep_slots);
                            }
                        }
                    }

                    let _ = broadcast_tx.send((CommitmentLevel::Finalized, Arc::new(vec![(msgid, message)])));
                }
                else => break,
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn client_loop(
        id: usize,
        endpoint: String,
        stream_tx: mpsc::Sender<TonicResult<FilteredUpdate>>,
        mut client_rx: mpsc::UnboundedReceiver<Option<(Option<u64>, Filter)>>,
        mut snapshot_rx: Option<crossbeam_channel::Receiver<Box<Message>>>,
        mut messages_rx: broadcast::Receiver<BroadcastedMessage>,
        replay: Arc<RwLock<ReplayStorage>>,
        debug_client_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
        drop_client: impl FnOnce(),
    ) {
        let mut filter = Filter::default();
        metrics::update_subscriptions(&endpoint, None, Some(&filter));

        metrics::connections_total_inc();
        DebugClientMessage::maybe_send(&debug_client_tx, || DebugClientMessage::UpdateFilter {
            id,
            filter: Box::new(filter.clone()),
        });
        info!("client #{id}: new");

        let mut is_alive = true;
        if let Some(snapshot_rx) = snapshot_rx.take() {
            Self::client_loop_snapshot(
                id,
                &endpoint,
                &stream_tx,
                &mut client_rx,
                snapshot_rx,
                &mut is_alive,
                &mut filter,
            )
            .await;
        }

        if is_alive {
            let mut replay_watermark = None;
            'outer: loop {
                tokio::select! {
                    mut message = client_rx.recv() => {
                        // forward to latest filter
                        loop {
                            match client_rx.try_recv() {
                                Ok(message_new) => {
                                    message = Some(message_new);
                                }
                                Err(mpsc::error::TryRecvError::Empty) => break,
                                Err(mpsc::error::TryRecvError::Disconnected) => {
                                    message = None;
                                    break;
                                }
                            }
                        }

                        match message {
                            Some(Some((from_slot, filter_new))) => {
                                metrics::update_subscriptions(&endpoint, Some(&filter), Some(&filter_new));
                                filter = filter_new;
                                DebugClientMessage::maybe_send(&debug_client_tx, || DebugClientMessage::UpdateFilter { id, filter: Box::new(filter.clone()) });
                                info!("client #{id}: filter updated");

                                if let Some(from_slot) = from_slot {
                                    let snapshot = {
                                        let replay = replay.read().await;
                                        replay.snapshot_from(from_slot)
                                    };

                                    let (messages, watermark) = match snapshot {
                                        Ok(value) => value,
                                        Err(slot) => {
                                            info!("client #{id}: broadcast from {from_slot} is not available");
                                            tokio::spawn(async move {
                                                let message = format!(
                                                    "broadcast from {from_slot} is not available, last available: {slot}"
                                                );
                                                let _ = stream_tx.send(Err(Status::internal(message))).await;
                                            });
                                            break 'outer;
                                        }
                                    };

                                    for (_msgid, message) in messages {
                                        for message in filter.get_updates(&message) {
                                            match stream_tx.send(Ok(message)).await {
                                                Ok(()) => {}
                                                Err(mpsc::error::SendError(_)) => {
                                                    error!("client #{id}: stream closed");
                                                    break 'outer;
                                                }
                                            }
                                        }
                                    }
                                    replay_watermark = watermark;
                                }
                            }
                            Some(None) => {
                                break 'outer;
                            },
                            None => {
                                break 'outer;
                            }
                        }
                    }
                    message = messages_rx.recv() => {
                        let messages = match message {
                            Ok((_, messages)) => messages,
                            Err(broadcast::error::RecvError::Closed) => {
                                break 'outer;
                            },
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                info!("client #{id}: lagged to receive geyser messages");
                                tokio::spawn(async move {
                                    let _ = stream_tx.send(Err(Status::internal("lagged to receive geyser messages"))).await;
                                });
                                break 'outer;
                            }
                        };

                        for (msgid, message) in messages.iter() {
                            if replay_watermark.is_some_and(|watermark| *msgid <= watermark) {
                                continue;
                            }

                            for message in filter.get_updates(message) {
                                match stream_tx.try_send(Ok(message)) {
                                    Ok(()) => {}
                                    Err(mpsc::error::TrySendError::Full(_)) => {
                                        error!("client #{id}: lagged to send an update");
                                        tokio::spawn(async move {
                                            let _ = stream_tx.send(Err(Status::internal("lagged to send an update"))).await;
                                        });
                                        break 'outer;
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => {
                                        error!("client #{id}: stream closed");
                                        break 'outer;
                                    }
                                }
                            }
                        }

                        if debug_client_tx.is_some() {
                            for message in messages.iter() {
                                if let Message::Slot(slot_message) = &message.1 {
                                    DebugClientMessage::maybe_send(&debug_client_tx, || DebugClientMessage::UpdateSlot { id, slot: slot_message.slot });
                                }
                            }
                        }
                    }
                }
            }
        }

        metrics::connections_total_dec();
        DebugClientMessage::maybe_send(&debug_client_tx, || DebugClientMessage::Removed { id });
        metrics::update_subscriptions(&endpoint, Some(&filter), None);
        info!("client #{id}: removed");
        drop_client();
    }

    async fn client_loop_snapshot(
        id: usize,
        endpoint: &str,
        stream_tx: &mpsc::Sender<TonicResult<FilteredUpdate>>,
        client_rx: &mut mpsc::UnboundedReceiver<Option<(Option<u64>, Filter)>>,
        snapshot_rx: crossbeam_channel::Receiver<Box<Message>>,
        is_alive: &mut bool,
        filter: &mut Filter,
    ) {
        info!("client #{id}: going to receive snapshot data");

        // we start with default filter, for snapshot we need wait actual filter first
        while *is_alive {
            match client_rx.recv().await {
                Some(Some((_from_slot, filter_new))) => {
                    if let Some(msg) = filter_new.get_pong_msg() {
                        if stream_tx.send(Ok(msg)).await.is_err() {
                            error!("client #{id}: stream closed");
                            *is_alive = false;
                        }
                        continue;
                    }

                    metrics::update_subscriptions(endpoint, Some(filter), Some(&filter_new));
                    *filter = filter_new;
                    info!("client #{id}: filter updated");
                    break;
                }
                Some(None) => {
                    *is_alive = false;
                }
                None => {
                    *is_alive = false;
                }
            };
        }

        while *is_alive {
            let message = match snapshot_rx.try_recv() {
                Ok(message) => {
                    metrics::message_queue_size_dec();
                    message
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    sleep(Duration::from_millis(1)).await;
                    continue;
                }
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    info!("client #{id}: end of startup");
                    break;
                }
            };

            for message in filter.get_updates(&message) {
                if stream_tx.send(Ok(message)).await.is_err() {
                    error!("client #{id}: stream closed");
                    *is_alive = false;
                    break;
                }
            }
        }
    }
}

#[tonic::async_trait]
impl Geyser for GrpcService {
    type SubscribeStream = ReceiverStream<TonicResult<FilteredUpdate>>;

    async fn subscribe(
        &self,
        mut request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        let id = self.subscribe_id.fetch_add(1, Ordering::Relaxed);

        let x_request_snapshot = request.metadata().contains_key("x-request-snapshot");
        let snapshot_rx = if x_request_snapshot {
            self.snapshot_rx.lock().await.take()
        } else {
            None
        };
        let (stream_tx, stream_rx) = mpsc::channel(if snapshot_rx.is_some() {
            self.config_snapshot_client_channel_capacity
        } else {
            self.config_channel_capacity
        });
        let (client_tx, client_rx) = mpsc::unbounded_channel();
        let notify_exit1 = Arc::new(Notify::new());
        let notify_exit2 = Arc::new(Notify::new());

        let ping_stream_tx = stream_tx.clone();
        let ping_client_tx = client_tx.clone();
        let ping_exit = Arc::clone(&notify_exit1);
        tokio::spawn(async move {
            let exit = ping_exit.notified();
            tokio::pin!(exit);

            loop {
                tokio::select! {
                    _ = &mut exit => {
                        break;
                    }
                    _ = sleep(Duration::from_secs(10)) => {
                        let msg = FilteredUpdate::new_empty(FilteredUpdateOneof::ping());
                        match ping_stream_tx.try_send(Ok(msg)) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {}
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                let _ = ping_client_tx.send(None);
                                break;
                            }
                        }
                    }
                }
            }
        });

        let endpoint = request
            .metadata()
            .get("x-endpoint")
            .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
            .unwrap_or_else(|| "".to_owned());

        let config_filter_limits = Arc::clone(&self.config_filter_limits);
        let filter_names = Arc::clone(&self.filter_names);
        let incoming_stream_tx = stream_tx.clone();
        let incoming_client_tx = client_tx;
        let incoming_exit = Arc::clone(&notify_exit2);
        tokio::spawn(async move {
            let exit = incoming_exit.notified();
            tokio::pin!(exit);

            loop {
                tokio::select! {
                    _ = &mut exit => {
                        break;
                    }
                    message = request.get_mut().message() => match message {
                        Ok(Some(request)) => {
                            let mut filter_names = filter_names.lock().await;
                            filter_names.try_clean();

                            if let Err(error) = match Filter::new(&request, &config_filter_limits, &mut filter_names) {
                                Ok(filter) => {
                                    if let Some(msg) = filter.get_pong_msg() {
                                        if incoming_stream_tx.send(Ok(msg)).await.is_err() {
                                            error!("client #{id}: stream closed");
                                            let _ = incoming_client_tx.send(None);
                                            break;
                                        }
                                        continue;
                                    }

                                    match incoming_client_tx.send(Some((request.from_slot, filter))) {
                                        Ok(()) => Ok(()),
                                        Err(error) => Err(error.to_string()),
                                    }
                                },
                                Err(error) => Err(error.to_string()),
                            } {
                                let err = Err(Status::invalid_argument(format!(
                                    "failed to create filter: {error}"
                                )));
                                if incoming_stream_tx.send(err).await.is_err() {
                                    let _ = incoming_client_tx.send(None);
                                }
                            }
                        }
                        Ok(None) => {
                            break;
                        }
                        Err(_error) => {
                            let _ = incoming_client_tx.send(None);
                            break;
                        }
                    }
                }
            }
        });

        tokio::spawn(Self::client_loop(
            id,
            endpoint,
            stream_tx,
            client_rx,
            snapshot_rx,
            self.broadcast_tx.subscribe(),
            Arc::clone(&self.replay),
            self.debug_clients_tx.clone(),
            move || {
                notify_exit1.notify_one();
                notify_exit2.notify_one();
            },
        ));

        Ok(Response::new(ReceiverStream::new(stream_rx)))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        let count = request.get_ref().count;
        let response = PongResponse { count };
        Ok(Response::new(response))
    }

    async fn get_latest_blockhash(
        &self,
        request: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        if let Some(blocks_meta) = &self.blocks_meta {
            blocks_meta
                .get_block(
                    |block| {
                        block.block_height.map(|value| GetLatestBlockhashResponse {
                            slot: block.slot,
                            blockhash: block.blockhash.clone(),
                            last_valid_block_height: value.block_height
                                + MAX_RECENT_BLOCKHASHES as u64,
                        })
                    },
                    request.get_ref().commitment,
                )
                .await
        } else {
            Err(Status::unimplemented("method disabled"))
        }
    }

    async fn get_block_height(
        &self,
        request: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        if let Some(blocks_meta) = &self.blocks_meta {
            blocks_meta
                .get_block(
                    |block| {
                        block.block_height.map(|value| GetBlockHeightResponse {
                            block_height: value.block_height,
                        })
                    },
                    request.get_ref().commitment,
                )
                .await
        } else {
            Err(Status::unimplemented("method disabled"))
        }
    }

    async fn get_slot(
        &self,
        request: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        if let Some(blocks_meta) = &self.blocks_meta {
            blocks_meta
                .get_block(
                    |block| Some(GetSlotResponse { slot: block.slot }),
                    request.get_ref().commitment,
                )
                .await
        } else {
            Err(Status::unimplemented("method disabled"))
        }
    }

    async fn is_blockhash_valid(
        &self,
        request: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        if let Some(blocks_meta) = &self.blocks_meta {
            let req = request.get_ref();
            blocks_meta
                .is_blockhash_valid(&req.blockhash, req.commitment)
                .await
        } else {
            Err(Status::unimplemented("method disabled"))
        }
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Ok(Response::new(GetVersionResponse {
            version: serde_json::to_string(&GrpcVersionInfo::default()).unwrap(),
        }))
    }
}
