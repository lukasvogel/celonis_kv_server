package kvclient;

import javafx.beans.property.Property;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
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


public class Controller {

    @FXML
    public TextArea keyField;
    @FXML
    public TextArea valueField;

    private Property<String> key = new SimpleStringProperty();
    private Property<String> value =  new SimpleStringProperty();


    private HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build();


    @FXML
    public void initialize() {
        keyField.textProperty().bindBidirectional(key);
        valueField.textProperty().bindBidirectional(value);
    }

    private static final String SERVER_URI = "http://localhost:9080/kv/";


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




    public String get(String key) {

        HttpRequest request;
        try {
            request = HttpRequest.newBuilder()
                    .uri(new URI(SERVER_URI + URLEncoder.encode(key, "UTF-8")))
                    .GET()
                    .build();
        } catch (URISyntaxException | UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Key is malformed");
        }

        HttpResponse<String> response;
        try {
            response = client.send(request, HttpResponse.BodyHandler.asString());
            if (response.statusCode() != 200) {
                // Not our fault, the server should always accept keys.
                throw new IllegalArgumentException("Key not found: " + key + ", server returned with: " + response.statusCode());
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
        return response.body();
    }

    @FXML
    public void put(String key, String value) {
        HttpRequest request;
        try {
            request = HttpRequest.newBuilder()
                    .uri(new URI(SERVER_URI + URLEncoder.encode(key, "UTF-8")))
                    .PUT(HttpRequest.BodyProcessor.fromString(value))
                    .build();
        } catch (URISyntaxException | UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Key is malformed");
        }

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandler.asString());
            if (response.statusCode() != 200) {
                // Not our fault, the server should always accept keys.
                throw new IllegalStateException("PUT failed because of an internal server error, sorry");
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @FXML
    public void delete(String key) {
        HttpRequest request;
        try {
            request = HttpRequest.newBuilder()
                    .uri(new URI(SERVER_URI + URLEncoder.encode(key, "UTF-8")))
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


}
