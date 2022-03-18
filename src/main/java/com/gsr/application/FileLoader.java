package com.gsr.application;

import java.io.File;
import java.io.IOException;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

public class FileLoader {


    public List<String> readFileEntries(String fileName) {

        try {
            File file = loadFileFromResourceFolder(fileName);
           return Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.out.println("Hopeless case -- failed to read the file even though it was found... How did you end up here?");
            e.printStackTrace();
            System.exit(-1);
        } catch (IllegalArgumentException e ){
            System.out.println("Hopeless case -- failed to get the file as resource from the resource path");
            e.printStackTrace();
            System.exit(-1);
        }catch (URISyntaxException e ){
            System.out.println("Hopeless case -- File " + fileName + " has an invalid URI syntax");
            e.printStackTrace();
            System.exit(-1);
        }

         return null;
    }


    /**
     * Don't use this in a jar-file... it won't wokr
     * @param fileName market data to load
     * @return a file which can easily be read
     */
    private File loadFileFromResourceFolder(String fileName) throws URISyntaxException {

        URL resource = this.getClass().getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file not found! " + fileName);
        } else {
            return new File(resource.toURI());
        }

    }
}
