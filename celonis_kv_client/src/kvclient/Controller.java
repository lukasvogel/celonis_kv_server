package kvclient;

import javafx.beans.property.Property;
import javafx.beans.property.SimpleStringProperty;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.TextArea;
import jdk.incubator.http.HttpClient;
import jdk.incubator.http.HttpRequest;
import jdk.incubator.http.HttpResponse;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;

import static javafx.application.Platform.exit;


public class Controller {

    @FXML
    public TextArea keyField;
    @FXML
    public TextArea valueField;

    private Property<String> key = new SimpleStringProperty();
    private Property<String> value = new SimpleStringProperty();


    private HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build();


    @FXML
    public void initialize() {
        keyField.textProperty().bindBidirectional(key);
        valueField.textProperty().bindBidirectional(value);
    }

    private static final String SERVER_URI = "http://localhost:9080/";
    private static final String API_PREFIX = "kv/";
    private static final String CONTROL_PREFIX = "control/";


    @FXML
    public void handleGetAction(ActionEvent e) {
        System.out.println("GET: " + key.getValue());
        value.setValue(get(key.getValue()));
    }

    @FXML
    public void handlePutAction(ActionEvent e) {
        System.out.println("PUT: " + key.getValue() + " -> " + value.getValue());

        put(key.getValue(), value.getValue());
    }

    @FXML
    public void handleDeleteAction(ActionEvent e) {
        delete(key.getValue());
    }

    @FXML
    public void handleShutdownAction(ActionEvent e) {
        shutdown();
    }




    private String get(String key) {

        HttpRequest request;
        try {
            request = HttpRequest.newBuilder()
                    .uri(new URI(SERVER_URI + API_PREFIX + URLEncoder.encode(key, "UTF-8")))
                    .GET()
                    .build();
        } catch (URISyntaxException | UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Key is malformed");
        }

        HttpResponse<String> response;
        try {
            response = client.send(request, HttpResponse.BodyHandler.asString());
            if (response.statusCode() != 200) {
                return "";
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return "";
        }
        return response.body();
    }

    private void put(String key, String value) {
        HttpRequest request;
        try {
            request = HttpRequest.newBuilder()
                    .uri(new URI(SERVER_URI + API_PREFIX + URLEncoder.encode(key, "UTF-8")))
                    .PUT(HttpRequest.BodyProcessor.fromString(value))
                    .build();
        } catch (URISyntaxException | UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Key is malformed");
        }

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandler.asString());
            if (response.statusCode() != 200) {
                // Not our fault, the server should always accept keys.
                System.out.println(response.statusCode());
                throw new IllegalStateException("PUT failed because of an internal server error, sorry");
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void delete(String key) {
        HttpRequest request;
        try {
            request = HttpRequest.newBuilder()
                    .uri(new URI(SERVER_URI + API_PREFIX + URLEncoder.encode(key, "UTF-8")))
                    .DELETE(HttpRequest.noBody())
                    .build();
        } catch (URISyntaxException | UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Key is malformed");
        }

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandler.asString());
            if (response.statusCode() != 200) {
                // Not our fault, the server should always be able to delete keys even if they are not set.
                throw new IllegalStateException("DELETE failed because of an internal server error, sorry");
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }


    private void shutdown() {
        HttpRequest request;
        try {
            request = HttpRequest.newBuilder()
                    .uri(new URI(SERVER_URI + CONTROL_PREFIX + "stop"))
                    .POST(HttpRequest.noBody())
                    .build();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Command is malformed");
        }

        try {
            client.send(request, HttpResponse.BodyHandler.asString());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        exit();

    }





}
