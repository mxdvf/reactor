#[macro_export]
macro_rules! spawn_operator {
    (
        $op_type:ty,
        $op_enum:expr,
        $state:ty,
        $in_row:ty,
        $out_row:ty,
        $spawn_args: expr,
        $local_qs: expr,
        $ckpt: expr,
        $cancel_token: expr,
        $task_tracker: expr,
        $inst: expr
    ) => {
        let _ = $crate::env_logger::builder().is_test(true).try_init();

        let op_args = $crate::tensile_core::operator::OpArgs {
            idx: $spawn_args.my_info.idx(),
            num_peers: $spawn_args.my_info.num_peers(),
            user_args: $spawn_args.op_args.clone(),
        };
        let op = match <$op_type>::try_from(op_args) {
            Ok(op_instance) => op_instance,
            Err(err) => return Box::new(Err(err)),
        };
        let op: $crate::tensile_core::operator::OperatorEnum<$state, $in_row, $out_row> =
            $op_enum(Box::new(op));

        RUNTIME.spawn($crate::worker::Worker::start(
            op,
            $ckpt,
            $spawn_args,
            $local_qs,
            $cancel_token,
            $task_tracker,
            $inst,
        ));
    };
}

macro_rules! register_ops {
    ($($name:ty, $op_type:expr, $op_state:ty, $op_in:ty, $op_out:ty);* $(;)?) => {
        type OpSpawner = fn(
            Box<$crate::tensile_core::service::WorkerSpawnInfo>,
            Box<LocalWorkerQ>,
            Box<$crate::tensile_core::checkpoint::CheckpointBackends>,
            Box<$crate::CancellationToken>,
            Box<$crate::TaskTracker>,
            Box<$crate::tensile_instrumentation::Instrumenter<$crate::tensile_instrumentation::InstWorker>>,
        ) -> Box<Result<(), String>>;
        $crate::lazy_static::lazy_static! {
            static ref RUNTIME: $crate::tokio::runtime::Runtime = $crate::tokio::runtime::Runtime::new().unwrap();
        }
        $crate::lazy_static::lazy_static! {
            static ref REGISTRY: HashMap<String, OpSpawner> = {
                let mut m = HashMap::new();
                $(
                    let f: OpSpawner = |spawn_args: Box<$crate::tensile_core::service::WorkerSpawnInfo>,
                                        localq: Box<LocalWorkerQ>,
                                        ckpt_backend: Box<$crate::tensile_core::checkpoint::CheckpointBackends>,
                                        cancel_token: Box<$crate::CancellationToken>,
                                        task_tracker: Box<$crate::TaskTracker>,
                                        instrumenter: Box<$crate::tensile_instrumentation::Instrumenter<$crate::tensile_instrumentation::InstWorker>>|
                        -> Box<Result<(), String>> {
                            $crate::spawn_operator!(
                                $name,
                                $op_type,
                                $op_state,
                                $op_in,
                                $op_out,
                                *spawn_args,
                                *localq,
                                *ckpt_backend,
                                *cancel_token,
                                *task_tracker,
                                *instrumenter
                            );
                            Box::new(Ok(()))
                        };
                    m.insert(stringify!($name).to_string(), f);
                )*
                m
            };
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn spawn(
            spawn_args: Box<$crate::tensile_core::service::WorkerSpawnInfo>,
            localq: Box<LocalWorkerQ>,
            ckpt_backend: Box<$crate::tensile_core::checkpoint::CheckpointBackends>,
            cancel_token: Box<$crate::CancellationToken>,
            task_tracker: Box<$crate::TaskTracker>,
            instrumenter: Box<$crate::tensile_instrumentation::Instrumenter<$crate::tensile_instrumentation::InstWorker>>,
        ) -> Box<Result<(), String>> {
            if let Some(op) = REGISTRY.get(&spawn_args.op_name) {
                op(spawn_args, localq, ckpt_backend, cancel_token, task_tracker, instrumenter)
            } else {
                Box::new(Err("Op not found".to_string()))
            }
        }

    };
}
