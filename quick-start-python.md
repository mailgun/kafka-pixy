To use Kafka-Pixy from a Python application you need to:

1. Add `grpcio>=1.2.0` as a dependency to your setup.py or/and requirements.txt.

2. Install `grpcio` to your Python Virtual Environment:

    ```bash
    pip install grpcio>=1.2.0
    ```

3. Create `kafkapixy` package in your application and copy
 [kafkapixy_pb2.py](https://github.com/mailgun/kafka-pixy/blob/master/gen/python/kafkapixy_pb2.py) and
 [kafkapixy_pb2_grpc.py](https://github.com/mailgun/kafka-pixy/blob/master/gen/python/kafkapixy_pb2_grpc.py)
 files to it.

4. Add imports:

    ```python
    import grpc
    from kafkapixy.kafkapixy_pb2 import ProdRq, ConsNAckRq, AckRq
    from kafkapixy.kafkapixy_pb2_grpc import KafkaPixyStub
    ```

5. Create a Kafka-Pixy client:

    ```python
    # Kafka-Pixy is supposed to be running on the same host hence 127.0.0.1,
    # and by default it listens on port 19091.
    grpc_chan = grpc.insecure_channel("127.0.0.1:19091")
    # The client is thread safe so you need only one.
    global _kp_clt
    _kp_clt = KafkaPixyStub(grpc_chan) 
    ```
    
6. To produce message it is recommended to write a thin wrapper around
 `KafkaPixyStub.Produce` function to tailor it for your particular use case.
 Use the following template as a starting point:

    ```python
    def produce(topic, key, msg):
        # Refer to ProdRq definition in kafkapixy.proto for complete list of
        # parameters and their semantic.
        rq = ProdRq(topic=topic, key_value=key, message=msg)
        try:
           # Make sure _PRODUCE_TIMEOUT is at least greater then 
           # (producer.flush_frequency + producer.retry_backoff) * producer.retry_max
           # as configured in the Kafka-Pixy config file.
           global _PRODUCE_TIMEOUT
           global _kp_clt
           rs = _kp_clt.Produce(rq, timeout=_PRODUCE_TIMEOUT)
        
           # The meaning of "success" depends on combination of ProdRq.async_mode
           # request parameter and producer.required_acks Kafka-Pixy config
           # parameter values. Refer to ProdRq definition in kafkapixy.proto for
           # details.
         
        except grpc.RpcError as err:
           if hasattr(err, 'code'):
               if err.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                   # Kafka-Pixy did not reply in _PRODUCE_TIMEOUT. Check if it is
                   # big enough to accommodate all possible retries.
                   return
                
               if err.code() == grpc.StatusCode.INVALID_ARGUMENT:
                   # Probably invalid topic, but check err.message for details.
                   return
                
           # Unexpected exception, check err.message for details.
           
    ```
    
7. To consume messages you need to run a consume-n-ack loop in a thread. It is
 usually started on application startup runs until the application is
 terminated. To increase throughput you can run several consume-n-ack threads,
 but the exact number should be selected based on performance measurements in
 each particular case. Please find a reference implementation of a
 consume-n-ack loop below:
    
    ```python
    def run_consume_n_ack(group, topic, msg_handler):
        """
        Runs consume-n-ack loop until global variable _keep_running is set to False. 
        """
        ack_partition = None
        ack_offset = None
    
        rq = ConsNAckRq(topic=topic, group=group)
        global _keep_running
        while _keep_running:
            if ack_offset is None:
                rq.no_ack = True
                rq.ack_partition = 0
                rq.ack_offset = 0
            else:
                rq.no_ack = False
                rq.ack_partition = ack_partition
                rq.ack_offset = ack_offset    
            
            try:
                # Make sure _CONSUME_TIMEOUT is at least greater then 
                # consumer.long_polling_timeout Kafka-Pixy config parameter value.
                global _CONSUME_TIMEOUT
                global _kp_clt
                rs = _kp_clt.ConsumeNAck(rq, timeout=_CONSUME_TIMEOUT)
            except Exception as e:
                if isinstance(e, grpc.RpcError) and hasattr(e, 'code'):
                    if e.code() == grpc.StatusCode.NOT_FOUND:
                        # Long polling timeout. The topic is empty. Just make
                        # another request.
                        ack_offset = None
                        continue
    
                # Unexpected errors can be generated in rapid succession e.g.
                # when a Kafka-Pixy is down. So it makes sense to back off.
                global _BACK_OFF_TIMEOUT
                sleep(_BACK_OFF_TIMEOUT)
                continue
    
            try:
                msg_handler(rs.message)
                ack_partition = rs.partition
                ack_offset = rs.offset
            except:
                ack_offset = None
                # The message handler raised an exception, it is up to you what
                # to do in this case.
    
        # If there is nothing to acknowledge then return.
        if ack_offset is None:
            return
    
        # Acknowledge the last consumed message.
        rq = AckRq(topic=topic,
                   group=group,
                   partition=ack_partition,
                   offset=ack_offset)
        try:
            global _kp_clt
            _kp_clt.Ack(rq, timeout=_CONSUME_TIMEOUT)
        except:
            _log.exception('Failed to ack last message: topic=%s, partition=%d, '
                           'offset=%d' % (topic, ack_partition, ack_offset))

    ``` 
