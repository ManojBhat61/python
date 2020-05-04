from kafka import KafkaProducer
import sys
import logging

try:

    ########### Use the debug line if you need to know what's going on
    #logging.basicConfig(level=logging.DEBUG)

    '''
    This is working with bitnami kafka running on port 192.168.1.59.  

    ##############
    # Notes
    #   sasl_plain_username :  this is the value of 'username' from the KafkaClient or KafkaServer sections of kafka_jaas.conf.
    #  NOTE: The username in Client section does not work for some reason.
    #
    #   IP address/hostname : must be in the advertised_listeners of kafka server.properties.
    #   kafka server's /etc/hosts should have the IP address and hostname.  Using 127.0.0.1 will not work.
    #   NOTE: Bitnami kafka has a script ( in /opt/bitnami/scripts/init) called hostname, which runs everytime ctlscript.sh is run.
    #         modifies /etc/hosts to add 127.0.0.1  bitnami. This script has to be modified to replace 127.0.0.1 with the VM's IP addr.
    #
    #  using sasl_plaintext, the id and password are sent plaintext over the wire. You can see this in wireshark and in tcpdump.
    ###############
    '''
    producer = KafkaProducer(bootstrap_servers=['192.168.1.59:9092'],sasl_mechanism='PLAIN',
                             security_protocol='SASL_PLAINTEXT', sasl_plain_username='user',
                             sasl_plain_password='abcd1234')
    producer.send('test' , b'This is a test message from my PC to the broker running on the VM')
    producer.close()

except Exception as e:
    print('exception occurred ' , e)
except KeyboardInterrupt as ke:
    sys.exit()
   



