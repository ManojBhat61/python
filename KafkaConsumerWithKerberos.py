from kafka import KafkaConsumer
from kafka import TopicPartition
import logging
import sys

try:
    ########### Use the debug line if you need to know what's going on
    #logging.basicConfig(level=logging.DEBUG)

    '''

    ##############
    # Notes
    # exception occurred GSSAPI lib not available. Solution: Had to install gssapi package using pip. 
    #  NOTE: gssapi needs python.h to be present 
    
    # NOTE: Do a kinit before running
    
    auto_offset_reset='earliest' == for the from_beginning option
    partition # of 0 has to be specified. Otherwise it will pick some random one and won't see any messages. 
    
    ###############
    '''
    consumer = KafkaConsumer( bootstrap_servers=['centos7a:9092'], sasl_mechanism='GSSAPI',
                             security_protocol='SASL_PLAINTEXT', auto_offset_reset='earliest')


    consumer.max_buffer_size = 0
    consumer.assign([TopicPartition('test', 0)])   #this is necessary to specify the partition number
    s = consumer.assignment()
    print ("===============")


    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))
except Exception as e:
    print('exception occurred', e)
except KeyboardInterrupt as ke:
    sys.exit()
