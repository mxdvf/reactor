use tokio::sync::mpsc::{
    self,
    error::{SendError, TryRecvError},
};

pub static MAX_PRIO: usize = 0;

/// Messages that can flow between the actors.
//pub trait Msg: Send + std::fmt::Debug {}
pub trait HasPriority {
    fn priority(&self) -> usize {
        MAX_PRIO
    }
}

#[derive(Clone)]
pub struct PriorityChannelTx<T: HasPriority> {
    senders: Vec<mpsc::Sender<T>>,
}

pub struct PriorityChannelRx<T> {
    receivers: Vec<mpsc::Receiver<T>>,
}

impl<T: HasPriority> PriorityChannelTx<T> {
    pub async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        let idx = msg.priority();
        if let Some(tx) = self.senders.get(idx) {
            (*tx).send(msg).await
        } else {
            Err(SendError(msg))
        }
    }
    #[allow(unused)]
    pub fn remove_prio(&mut self) {
        self.senders.pop();
    }
}

impl<T> PriorityChannelRx<T> {
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let mut disconnected_count = 0;

        for rx in &mut self.receivers {
            match rx.try_recv() {
                Ok(msg) => return Ok(msg),
                Err(TryRecvError::Empty) => continue,
                Err(TryRecvError::Disconnected) => disconnected_count += 1,
            }
        }

        if disconnected_count == self.receivers.len() {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }
}

pub fn priority_channel<T: HasPriority>(
    num_prios: usize,
    channel_size: usize,
) -> (PriorityChannelTx<T>, PriorityChannelRx<T>) {
    let mut senders = Vec::new();
    let mut receivers = Vec::new();

    for _ in 0..num_prios {
        let (tx, rx) = mpsc::channel::<T>(channel_size);
        senders.push(tx);
        receivers.push(rx);
    }

    (
        PriorityChannelTx { senders },
        PriorityChannelRx { receivers },
    )
}

#[cfg(test)]
mod tests {
    use crate::R2PMsg;

    use super::*;

    // Dummy message type
    #[derive(Debug, Clone, PartialEq)]
    enum TestMsg {
        Low,
        Medium,
        High,
    }

    impl HasPriority for TestMsg {
        fn priority(&self) -> usize {
            match self {
                TestMsg::Low => 2,
                TestMsg::Medium => 1,
                TestMsg::High => MAX_PRIO,
            }
        }
    }

    #[tokio::test]
    async fn test_priority_order_stress() {
        let (tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>(3, 10000);

        for _ in 0..10000 {
            tx.send(R2PMsg::Msg(TestMsg::Low)).await.unwrap();
        }
        tx.send(R2PMsg::Msg(TestMsg::Medium)).await.unwrap();
        tx.send(R2PMsg::Msg(TestMsg::High)).await.unwrap();

        assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::High)));
        assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::Medium)));
        for _ in 0..10000 {
            assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::Low)));
        }
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn test_priority_order() {
        let (tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>(3, 100);

        tx.send(R2PMsg::Msg(TestMsg::Low)).await.unwrap();
        tx.send(R2PMsg::Msg(TestMsg::Medium)).await.unwrap();
        tx.send(R2PMsg::Msg(TestMsg::High)).await.unwrap();

        assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::High)));
        assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::Medium)));
        assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::Low)));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn test_disconnected_behavior() {
        let (tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>(2, 100);
        drop(tx.senders); // Drop all senders to simulate disconnect

        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[tokio::test]
    async fn test_exit_message() {
        let (tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>(3, 100);
        tx.send(R2PMsg::Exit).await.unwrap();
        tx.send(R2PMsg::Msg(TestMsg::Medium)).await.unwrap();

        assert_eq!(rx.try_recv(), Ok(R2PMsg::Exit));
        assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::Medium)));
    }

    #[tokio::test]
    async fn test_empty_channel() {
        let (_tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>(1, 100);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }
    // fn test_partial_disconnect_behavior() {
    //     let (mut tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>(2);

    //     // Drop only the High priority sender
    //     tx.senders.remove(Priority::High.to_index());
    //     println!("Getting the channel in high index");
    //     println!("{:?}", tx.senders.get(Priority::High.to_index()));
    //     // If you try to send to High now, it should fail (simulate disconnection)
    //     assert!(tx.senders.get(Priority::High.to_index()).is_none());

    //     // The channel should still be "connected" from the perspective of Medium/Low
    //     assert_ne!(rx.get_row(), PriorityChannelStatus::Disconnected);
    // }
    #[tokio::test]
    async fn test_partial_disconnect_behavior() {
        let (mut tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>(3, 100);

        tx.remove_prio();

        tx.send(R2PMsg::Msg(TestMsg::High)).await.unwrap();
        assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::High)));
        tx.send(R2PMsg::Msg(TestMsg::Medium)).await.unwrap();
        assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::Medium)));
        let msg = R2PMsg::Msg(TestMsg::Low);
        match tx.send(msg.clone()).await {
            Ok(_) => panic!(),
            Err(send_err) => {
                assert_eq!(send_err.0, msg);
            }
        }
    }
}
