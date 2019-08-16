package com.xiaoji.duan.ips;

import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.amqpbridge.AmqpBridgeOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

/**
 * 
 * 根据ip地址获取城市信息
 * 
 * @author xiaoji
 *
 */
public class MainVerticle extends AbstractVerticle {

	private WebClient client = null;
	private AmqpBridge bridge = null;
	private MongoClient mongodb = null;

	@Override
	public void start(Future<Void> startFuture) throws Exception {
		client = WebClient.create(vertx);

		JsonObject config = new JsonObject();
		config.put("host", "mongodb");
		config.put("port", 27017);
		config.put("keepAlive", true);
		mongodb = MongoClient.createShared(vertx, config);

		AmqpBridgeOptions options = new AmqpBridgeOptions();
		bridge = AmqpBridge.create(vertx, options);

		bridge.endHandler(endHandler -> {
			connectStompServer();
		});
		connectStompServer();

	}

	private void subscribeTrigger(String trigger) {
		MessageConsumer<JsonObject> consumer = bridge.createConsumer(trigger);
		System.out.println("Consumer " + trigger + " subscribed.");
		consumer.handler(vertxMsg -> this.process(trigger, vertxMsg));
	}

	private void process(String consumer, Message<JsonObject> received) {
		System.out.println("Consumer " + consumer + " received [" + received.body().encode() + "]");

		JsonObject data = received.body().getJsonObject("body");

		String ip = data.getJsonObject("context").getString("ip");
		String next = data.getJsonObject("context").getString("next");

		ips(consumer, ip, next, 1);

	}

	private void ips(String consumer, String ip, String nextTask, Integer retry) {

		String requesturi = config()
				.getString("ips.serveurl",
						"http://opendata.baidu.com/api.php?query=##ip##&co=&resource_id=6006&t=1412300361645&ie=utf8&oe=gbk&cb=op_aladdin_callback&format=json&tn=baidu&cb=jQuery1102026811896078288555_1412299994977&_=1412299994981")
				.replaceAll("##ip##", ip);

		mongodb.findOne("ips_ip_vs_location", new JsonObject().put("ip", ip), new JsonObject(), findOne -> {
			if (findOne.succeeded()) {
				JsonObject cached = findOne.result();

				if (cached != null && !cached.isEmpty()) {
					JsonObject nextctx = new JsonObject().put("context",
							new JsonObject().put("ip", ip).put("location", cached));

					MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
					producer.send(new JsonObject().put("body", nextctx));
					producer.end();
					System.out.println(
							"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "]");

				} else {
					// 如果数据库没有缓存地址, 或者数据已经过期, 访问接口请求
					client.getAbs(requesturi).send(handler -> {
						if (handler.succeeded()) {
							HttpResponse<Buffer> resp = handler.result();

							String result = resp.bodyAsString("GBK");

							int start = result.indexOf("(") + 1;
							int end = result.lastIndexOf(");");
							
							//未返回正确的内容
							if (end <= start) {
								JsonObject nextctx = new JsonObject().put("context", new JsonObject().put("ip", ip)
										.put("location", new JsonObject()));

								MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
								producer.send(new JsonObject().put("body", nextctx));
								producer.end();
								System.out.println("Consumer " + consumer + " send to [" + nextTask + "] result ["
										+ nextctx.encode() + "]");
								
								return;
							}
							
							String jsonstring = result.substring(start, end);

							JsonObject location = new JsonObject();

							if (jsonstring != null && jsonstring.startsWith("{") && jsonstring.endsWith("}")) {
								location = new JsonObject(jsonstring);

								location.put("ip", ip);

								mongodb.save("ips_ip_vs_location", location, save -> {
								});
							}

							JsonObject nextctx = new JsonObject().put("context",
									new JsonObject().put("ip", ip).put("location", location));

							MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
							producer.send(new JsonObject().put("body", nextctx));
							producer.end();
							System.out.println("Consumer " + consumer + " send to [" + nextTask + "] result ["
									+ nextctx.encode() + "]");

						} else {

							JsonObject nextctx = new JsonObject().put("context", new JsonObject().put("ip", ip)
									.put("location", new JsonObject()));

							MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
							producer.send(new JsonObject().put("body", nextctx));
							producer.end();
							System.out.println("Consumer " + consumer + " send to [" + nextTask + "] result ["
									+ nextctx.encode() + "]");
						}
					});

				}
			} else {

				// 如果数据库没有缓存地址, 或者数据已经过期, 访问接口请求
				client.getAbs(requesturi).send(handler -> {
					if (handler.succeeded()) {
						HttpResponse<Buffer> resp = handler.result();

						String result = resp.bodyAsString("GBK");

						int start = result.indexOf("(") + 1;
						int end = result.lastIndexOf(");");
						
						//未返回正确的内容
						if (end <= start) {
							JsonObject nextctx = new JsonObject().put("context", new JsonObject().put("ip", ip)
									.put("location", new JsonObject()));

							MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
							producer.send(new JsonObject().put("body", nextctx));
							producer.end();
							System.out.println("Consumer " + consumer + " send to [" + nextTask + "] result ["
									+ nextctx.encode() + "]");
							
							return;
						}
						
						String jsonstring = result.substring(start, end);

						JsonObject location = new JsonObject();

						if (jsonstring != null && jsonstring.startsWith("{") && jsonstring.endsWith("}")) {
							location = new JsonObject(jsonstring);

							location.put("ip", ip);

							mongodb.save("ips_ip_vs_location", location, save -> {
							});
						}

						JsonObject nextctx = new JsonObject().put("context",
								new JsonObject().put("ip", ip).put("location", location));

						MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
						producer.send(new JsonObject().put("body", nextctx));
						producer.end();
						System.out.println("Consumer " + consumer + " send to [" + nextTask + "] result ["
								+ nextctx.encode() + "]");

					} else {

						JsonObject nextctx = new JsonObject().put("context", new JsonObject().put("ip", ip)
								.put("location", new JsonObject()));

						MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
						producer.send(new JsonObject().put("body", nextctx));
						producer.end();
						System.out.println("Consumer " + consumer + " send to [" + nextTask + "] result ["
								+ nextctx.encode() + "]");
					}
				});

			}
		});

	}

	private void connectStompServer() {
		bridge.start(config().getString("stomp.server.host", "sa-amq"), config().getInteger("stomp.server.port", 5672),
				res -> {
					if (res.failed()) {
						res.cause().printStackTrace();
						if (!config().getBoolean("debug", true)) {
							connectStompServer();
						}
					} else {
						System.out.println("Stomp server connected.");
						subscribeTrigger(config().getString("amq.app.id", "ips"));
					}
				});

	}
	
}
