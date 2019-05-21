Proxy OPC-UA to MQTT Broker
===========================


This forwards URI-based OPC-UA subscriptions to a MQTT Broker.

## Usage


python2.7

    python gateway.py -h


docker

    docker run -it paulenglert/opc-mqtt-gateway:latest -h


Example:


    python gateway.py \
      --opc-host 'opcuaserver.com' --opc-port 48484 \
      --mqtt-host 'test.mosquitto.org' --mqtt-topic-prefix '/idatase' \
      --proxy-uri 'urn:unconfigured:application:Countries.AR.CENTENARIO.Temperature' \
      --proxy-uri 'urn:unconfigured:application:Countries.AR.CENTENARIO.WindSpeed' \
      --proxy-uri 'urn:unconfigured:application:Countries.AR.CENTENARIO.WindBearing'
