include config-ingest.mk  # Vars related to ingest step and spark parameters

SERVER_ASSEMBLY := server/target/scala-2.11/server-assembly-0.1.0.jar
INGEST_ASSEMBLY := ingest/target/scala-2.11/ingest-assembly-0.1.0.jar

ifeq ($(USE_SPOT),true)
MASTER_BID_PRICE:=BidPrice=${MASTER_PRICE},
WORKER_BID_PRICE:=BidPrice=${WORKER_PRICE},
BACKEND=accumulo
endif

ifdef COLOR
COLOR_TAG=--tags Color=${COLOR}
endif

ifndef CLUSTER_ID
CLUSTER_ID=$(shell if [ -e "cluster-id.txt" ]; then cat cluster-id.txt; fi)
endif

rwildcard=$(foreach d,$(wildcard $1*),$(call rwildcard,$d/,$2) $(filter $(subst *,%,$2),$d))

${SERVER_ASSEMBLY}: $(call rwildcard, server/src, *.scala) server/build.sbt
	./sbt "project server" assembly -no-colors
	@touch -m ${SERVER_ASSEMBLY}

${INGEST_ASSEMBLY}: $(call rwildcard, ingest/src, *.scala) ingest/build.sbt
	./sbt "project ingest" assembly -no-colors
	@touch -m ${INGEST_ASSEMBLY}

viewer/site.tgz: $(call rwildcard, viewer/components, *.js)
	@cd viewer && npm install && npm run build
	tar -czf viewer/site.tgz -C viewer/dist .

	aws emr add-steps --output text --cluster-id ${CLUSTER_ID} \
--steps Type=CUSTOM_JAR,Name="Ingest ${LAYER_NAME}",Jar=command-runner.jar,Args=[\
spark-submit,--master,yarn-cluster,\
--class,demo.SentinelIngestMain,\
--class,demo.SentinelUpdateMain,\
--driver-memory,${DRIVER_MEMORY},\
--driver-cores,${DRIVER_CORES},\
--executor-memory,${EXECUTOR_MEMORY},\
--executor-cores,${EXECUTOR_CORES},\
--conf,spark.dynamicAllocation.enabled=true,\
--conf,spark.yarn.executor.memoryOverhead=${YARN_OVERHEAD},\
--conf,spark.yarn.driver.memoryOverhead=${YARN_OVERHEAD}\
] | cut -f2 | tee last-step-id.txt

clean:
	./sbt clean -no-colors
	rm -rf viewer/site.tgz
	rm -rf viewer/dist/*

local-ingest: ${INGEST_ASSEMBLY}
	spark-submit --name "${NAME} Ingest" --master "local[4]" --driver-memory 10G \
	--driver-cores 1 \
	--executor-memory 8g \
	--executor-cores 1 \
	--conf spark.yarn.executor.memoryOverhead=1g \
	--conf spark.yarn.driver.memoryOverhead=1g \
	--conf spark.network.timeout=240s \
	--conf spark.driver.maxResultSize=5g \
	--conf spark.driver.extraJavaOptions="-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70" \
	--driver-java-options "-XX:+UseCompressedOops -XX:MaxPermSize=2g -d64 -Xms1g" \
${INGEST_ASSEMBLY} \
--backend-profiles "file:///${PWD}/conf/backend-profiles.json" \
--input "file://${PWD}/conf/input-local.json" \
--output "file://${PWD}/conf/output-local.json"

local-update: ${INGEST_ASSEMBLY}
	spark-submit --name "${NAME} Ingest" --master "local[4]" --driver-memory 10G \
	--driver-cores 1 \
	--executor-memory 8g \
	--executor-cores 1 \
	--conf spark.yarn.executor.memoryOverhead=1g \
	--conf spark.yarn.driver.memoryOverhead=1g \
	--conf spark.network.timeout=240s \
	--conf spark.driver.maxResultSize=5g \
	--conf spark.driver.extraJavaOptions="-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70" \
	--driver-java-options "-XX:+UseCompressedOops -XX:MaxPermSize=2g -d64 -Xms1g" \
${INGEST_ASSEMBLY} \
--backend-profiles "file:///${PWD}/conf/backend-profiles.json" \
--input "file://${PWD}/conf/input-local.json" \
--output "file://${PWD}/conf/output-local.json"

local-tile-server: CATALOG=catalog
local-tile-server: ZOOS=localhost
local-tile-server: MASTER=localhost
local-tile-server: ${SERVER_ASSEMBLY}
	spark-submit --name "${NAME} Service" --master "local" --driver-memory 1G \
${SERVER_ASSEMBLY} cassandra ${ZOOS} ${MASTER}

define UPSERT_BODY
{
  "Changes": [{
    "Action": "UPSERT",
    "ResourceRecordSet": {
      "Name": "${1}",
      "Type": "CNAME",
      "TTL": 300,
      "ResourceRecords": [{
        "Value": "${2}"
      }]
    }
  }]
}
endef

local-webui-py3:
	cd viewer; npm install && npm run build && cd ./dist && python -m http.server 8000	

local-webui-py2:
	cd viewer; npm install && npm run build && cd ./dist && python -m SimpleHTTPServer 8000	

.PHONY: local-ingest ingest local-tile-server update-route53 get-logs
