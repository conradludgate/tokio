cfg_rt! {
    use crate::util::trace::SpawnMeta;

    cfg_usdt! {
        use core::{
            pin::Pin,
            task::{Context, Poll},
        };
        use std::mem;
        use pin_project_lite::pin_project;
        use std::future::Future;

        #[inline]
        pub(crate) fn task<F>(task: F, kind: &'static str, meta: SpawnMeta<'_>, id: u64) -> Instrumented<F> {
            fn probe(kind: &'static str, meta: SpawnMeta<'_>, id: u64, size: usize) {
                probes::task__start_eager!(|| (
                    id,
                    (kind == "task") as u8,
                    size,
                    meta.original_size,
                ));
                probes::task__details!(|| (
                    id,
                    meta.name.unwrap_or_default(),
                    meta.spawned_at.0.file(),
                    meta.spawned_at.0.line(),
                    meta.spawned_at.0.column(),
                ));
            }

            probe(kind, meta, id, mem::size_of::<F>());

            Instrumented {
                inner: task,
                task_id: id,
            }
        }

        pin_project! {
            #[derive(Debug, Clone)]
            pub(crate) struct Instrumented<F> {
                #[pin]
                inner: F,
                task_id: u64,
            }

            impl<F> PinnedDrop for Instrumented<F> {
                fn drop(this: Pin<&mut Self>) {
                    let this = this.project();
                    probes::task__terminate_eager!(|| *this.task_id);
                }
            }
        }

        impl<F: Future> Future for Instrumented<F> {
            type Output = F::Output;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.project();
                probes::task__poll__start_eager!(|| *this.task_id);
                let res = this.inner.poll(cx);
                probes::task__poll__end_eager!(|| *this.task_id);
                res
            }
        }

        #[usdt::provider(provider = "tokio")]
        #[allow(non_snake_case)]
        pub(crate) mod probes {
            fn task__details(task_id: u64, name: &str, file: &str, line: u32, col: u32) {}

            // spwaned == 0 for block_on
            // spawned == 1 for task
            fn task__start(task_id: u64, spawned: u8, size: usize, original_size: usize) {}
            fn task__poll__start(task_id: u64) {}
            fn task__poll__end(task_id: u64) {}
            fn task__terminate(task_id: u64) {}

            fn task__waker__clone(task_id: u64) {}
            fn task__waker__wake(task_id: u64) {}
            fn task__waker__drop(task_id: u64) {}
        }
    }

    cfg_not_usdt! {
        #[inline]
        pub(crate) fn task<F>(task: F, _kind: &'static str, _meta: SpawnMeta<'_>, _id: u64) -> F {
            // nop
            task
        }
    }
}
