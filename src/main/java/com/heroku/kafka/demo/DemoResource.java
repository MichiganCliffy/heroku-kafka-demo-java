package com.heroku.kafka.demo;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;

@Path("/")
public class DemoResource {
  private final DemoProducer producer;

  private final DemoConsumer consumer;

  public DemoResource(DemoProducer producer, DemoConsumer consumer) {
    this.producer = producer;
    this.consumer = consumer;
  }

  @GET
  @Path("messages")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed
  public List<DemoMessage> getMessages() {
    return Lists.reverse(consumer.getMessages());
  }

  @POST
  @Path("messages")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Timed
  public Response addMessage(DemoMessage message) throws TimeoutException, ExecutionException {
    Uninterruptibles.getUninterruptibly(producer.send(message.getMessage()), 20, TimeUnit.SECONDS);
    return Response.ok() //200
      .entity(format("received message: %s", message))
      .type(MediaType.APPLICATION_JSON)
			.header("Access-Control-Allow-Origin", "*")
			.header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT, OPTIONS")
      .header("Access-Control-Allow-Headers", "origin, content-type, accept")
			.allow("OPTIONS").build();

    // return format("received message: %s", message);
  }
}