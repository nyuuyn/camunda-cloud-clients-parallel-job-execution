package io.berndruecker.experiments.cloudclient.java;

import com.google.common.collect.Maps;
import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.ZeebeWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriBuilder;
import reactor.core.publisher.Flux;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.util.Map;
import java.util.function.Function;

@Component
public class RestInvocationWorker {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestInvocationWorker.class);

    private static final String PAYMENT_URL = "http://localhost:9090/";

    @Autowired
    private RestTemplate rest;

    @Autowired
    private JobCounter counter;

    @ZeebeWorker(type = "checkDeliveryDate_thomann")
    public void checkDeliveryDate_thomann(final JobClient client, final ActivatedJob job) {
        counter.init();
        LOGGER.info("Invoke REST call...");
        Map<String, Object> vars = job.getVariablesAsMap();
        boolean canOrder = false;
        if (!((String)vars.get("InputVariable_DeliveryDate")).startsWith("In ")) {
            canOrder = true;
        }
        Map<String, Object> results = Maps.newHashMap();
        results.put("CanOrder", canOrder);
        LOGGER.info("...finished. Complete Job...");
        client.newCompleteCommand(job.getKey()).variables(results).send()
                .join();
        counter.inc();
    }

    @ZeebeWorker(type = "checkInStorage_thomann")
    public void checkInStorage_thomann(final JobClient client, final ActivatedJob job) {
        counter.init();
        LOGGER.info("Invoke REST call...");
        Map<String, Object> vars = job.getVariablesAsMap();

        ResponseEntity<String> response = rest.getForEntity(URI.create(String.valueOf(vars.get("InputVariable_URL"))), String.class);
        String body = response.getBody();
        int startIndex = body.indexOf("<span class=\"fx-availability fx-availability--on-date js-fx-tooltip-trigger\">");
        int endIndex = body.indexOf("</span>", startIndex);
        String value = body.substring(startIndex + "<span class=\"fx-availability fx-availability--on-date js-fx-tooltip-trigger\">".length(), endIndex).trim();        LOGGER.info("...finished. Complete Job...");
        Map<String, Object> results = Maps.newHashMap();
        results.put("InStorage", value);
        client.newCompleteCommand(job.getKey()).variables(results).send()
                .join();
        counter.inc();
    }

//    @ZeebeWorker(type = "rest")
    public void nonBlockingRestCall(final JobClient client, final ActivatedJob job) {
        counter.init();
        LOGGER.info("Invoke REST call...");
        Flux<String> paymentResponseFlux = WebClient.create()
                .get()
                .uri(PAYMENT_URL)
                .retrieve()
                .bodyToFlux(String.class);

        paymentResponseFlux.subscribe(
            response -> {
                LOGGER.info("...finished. Complete Job...");
                client.newCompleteCommand(job.getKey()).send()
                    .thenApply(jobResponse -> { counter.inc(); return jobResponse;})
                    .exceptionally(t -> {throw new RuntimeException("Could not complete job: " + t.getMessage(), t);});
            },
            exception -> {
                LOGGER.info("...REST invocation problem: " + exception.getMessage());
                client.newFailCommand(job.getKey())
                       .retries(1)
                       .errorMessage("Could not invoke REST API: " + exception.getMessage()).send()
                      .exceptionally(t -> {throw new RuntimeException("Could not fail job: " + t.getMessage(), t);});
            });


    }

}
