package server;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.gson.*;
import com.google.gson.stream.JsonReader;


public class JsonServer {
    private Gson gson;
    private JsonReader jsonReader;
    private ReadWriteLock lock;
    private Lock readlock;
    private Lock writeLock;
    private String filepath;


    public JsonServer(String filepath) {

        this.filepath = filepath;
        this.gson = new Gson();

        try {
            jsonReader = new JsonReader(new FileReader(filepath + "/db.json"));
        } catch (IOException e) {
            System.err.println("Could not create JsonReader");
            e.printStackTrace();
            System.exit(1);
        }
        lock = new ReentrantReadWriteLock();
        readlock = lock.readLock();
        writeLock = lock.writeLock();
    }

    private void resetJsonReader() {
        try {
            jsonReader.close();
            jsonReader = new JsonReader(new FileReader(filepath + "/db.json"));
        } catch (IOException e) {
            System.err.println("Issue resetting jsonReader");
        }
    }

    public boolean shutdown() {
        try {
            jsonReader.close();
            return true;
        } catch (IOException e) {
            System.err.println("Failed to shutdown Json Server: ");
            e.printStackTrace();
            return false;
        }
    }

    private boolean set(JsonArray key, JsonElement val) {
        writeLock.lock();
        JsonObject cache = gson.fromJson(jsonReader, JsonObject.class);
        if (key.size() < 2) {
            if (val.isJsonPrimitive()) {
                cache.addProperty(key.get(0).getAsString(), val.getAsString());
            } else {
                cache.add(key.get(0).getAsString(), gson.fromJson(val, JsonObject.class));
            }
        } else {
            JsonObject entry = cache.getAsJsonObject(key.get(0).getAsString());
            JsonElement cursor = entry;
            JsonArray existingLevels = new JsonArray(key.size()-1);
            JsonArray newLevels = new JsonArray(key.size()-1);
            int i = 1;
            for (; i < key.size(); i++) {
                JsonElement l = key.get(i);
                if (cursor.isJsonObject()) {
                    JsonObject cursObj = cursor.getAsJsonObject();
                    if (!cursObj.has(l.getAsString())) break;
                    else if (cursObj.get(l.getAsString()).isJsonPrimitive()) {
                        i++;
                        existingLevels.add(l);
                        break;
                    }
                    existingLevels.add(l);
                    cursor = cursObj.get(l.getAsString());
                }
            }
            for (; i < key.size(); i++) {
                //Add levels that need to be created
                newLevels.add(key.get(i));
            }

            JsonElement toAdd = val;
            for (int j = newLevels.size()-1; j >= 0; j--) {
                JsonObject temp = new JsonObject();
                if (toAdd.isJsonPrimitive()) {
                    temp.add(newLevels.get(j).getAsString(), toAdd.getAsJsonPrimitive());
                } else {
                    temp.add(newLevels.get(j).getAsString(), toAdd.getAsJsonObject());
                }
                toAdd = temp;
            }

            if (existingLevels.size() > 0) {
                if (cursor.isJsonObject()) {
                    JsonObject cursorObj = cursor.getAsJsonObject();
                    cursorObj.add(existingLevels.get(existingLevels.size()-1).getAsString(),
                                toAdd);

                } else {
                   JsonObject temp = entry;
                   int j = 0;
                   for (; !temp.has(cursor.getAsString()) && j < existingLevels.size(); j++) {
                      temp = temp.getAsJsonObject(existingLevels.get(j).getAsString());
                   }
                   temp.add(existingLevels.get(j).getAsString(), toAdd);
                }
            }
            cache.add(key.get(0).getAsString(), entry);
        }
        try (FileWriter writer = new FileWriter(filepath + "/db.json", false)) {
            gson.toJson(cache, writer);
        } catch (IOException e) {
            writeLock.unlock();
            return false;
        }
        resetJsonReader();
        writeLock.unlock();
        return true;
    }

    private JsonObject get(JsonArray key) {
        readlock.lock();
        JsonObject cache = gson.fromJson(jsonReader, JsonObject.class);
        resetJsonReader();
        readlock.unlock();
        if (!cache.has(key.get(0).getAsString())) {
            JsonObject err = new JsonObject();
            err.addProperty("ERROR", "No such key");
            return err;
        }
        JsonElement maybe = cache.get(key.get(0).getAsString());
        if (maybe.isJsonPrimitive()) {
            JsonObject ok = new JsonObject();
            ok.addProperty("response", "OK");
            ok.add("value", maybe.getAsJsonPrimitive());
            return ok;
        }
        JsonObject res = cache.getAsJsonObject(key.get(0).getAsString());
        if (key.size() > 1) {
            for (int i = 1; i < key.size(); i++) {
                if (!res.has(key.get(i).getAsString())) {
                    JsonObject err = new JsonObject();
                    err.addProperty("response","ERROR");
                    err.addProperty("reason", "No such key");
                    return err;
                }
                maybe = res.get(key.get(i).getAsString());
                if (maybe.isJsonPrimitive()) {
                    JsonObject ok = new JsonObject();
                    ok.addProperty("response", "OK");
                    ok.add("value", maybe); // TODO change to maybe.getAsString()?
                    return ok;
                }
                res = res.getAsJsonObject(key.get(i).getAsString());
            }
        }
        JsonObject ok = new JsonObject();
        ok.addProperty("response", "OK");
        ok.add("value", res);
        return ok;
        //return "OK " + res.getAsString();
    }

    private boolean delete(JsonArray key) {
        boolean result = true;
        System.out.println("--Delete 1");
        writeLock.lock();
        JsonObject cache = gson.fromJson(jsonReader, JsonObject.class);
        System.out.println("--Delete 2");
        if (!cache.has(key.get(0).getAsString())) {
            result = false;
        } else if (key.size() == 1) {
            cache.remove(key.get(0).getAsString());
        } else {
            System.out.println("--Delete 3");
            JsonObject toRemove = cache.getAsJsonObject(key.get(0).getAsString());
            System.out.println("--Delete 4");
            for (int i = 1; i < key.size() - 1; i++) {
                if (!toRemove.has(key.get(i).getAsString())) {
                    result = false;
                    break;
                }
                toRemove = toRemove.getAsJsonObject(key.get(i).getAsString());
            }
            if (!toRemove.has(key.get(key.size()-1).getAsString())) {
                result = false;
            } else {
                toRemove.remove(key.get(key.size()-1).getAsString());
            }
            System.out.println("--Delete 5");
        }
        try (FileWriter writer = new FileWriter(filepath + "/db.json", false)) {
            gson.toJson(cache, writer);
        } catch (IOException e) {
            resetJsonReader();
            writeLock.unlock();
            return false;
        }
        resetJsonReader();
        writeLock.unlock();
        return result;
    }

    public JsonObject execute(JsonObject cmdObj) {
        JsonObject result = new JsonObject();
        String cmdType = cmdObj.get("type").getAsString();

         if (cmdType.equalsIgnoreCase("set")) {
            if (!(cmdObj.has("value") && cmdObj.has("key"))) {
                JsonObject err = new JsonObject();
                err.addProperty("ERROR", "Wrong parameters");
                return err;
            }
             JsonElement keyElm = cmdObj.get("key");
             JsonArray keyPath;
             if (keyElm.isJsonArray()) {
                 keyPath = keyElm.getAsJsonArray();
             } else {
                 keyPath = new JsonArray(1);
                 keyPath.add(keyElm);
             }
            if (set(keyPath, cmdObj.get("value"))) result.addProperty("response", "OK");
            else {
                result.addProperty("response", "ERROR");
                result.addProperty("reason", "Set operation failed");
            }
         } else if (cmdType.equalsIgnoreCase("get")) {
            JsonElement keyElm = cmdObj.get("key");
            JsonArray keyPath;
            if (keyElm.isJsonArray()) {
                keyPath = keyElm.getAsJsonArray();
            } else {
                keyPath = new JsonArray(1);
                keyPath.add(keyElm);
            }
            if (!cmdObj.has("key")) {
                JsonObject err = new JsonObject();
                err.addProperty("ERROR", "Wrong parameters");
                return err;
            }
            result = get(keyPath);
        } else if (cmdType.equalsIgnoreCase("delete")) {
            if (cmdObj.size() == 2) {
                JsonElement keyElm = cmdObj.get("key");
                JsonArray keyPath;
                if (keyElm.isJsonArray()) {
                    keyPath = keyElm.getAsJsonArray();
                } else {
                    keyPath = new JsonArray(1);
                    keyPath.add(keyElm);
                }
                if (delete(keyPath)) {
                    result.addProperty("response", "OK");
                } else {
                    result.addProperty("ERROR", "No such key");
                }
            } else {
                JsonObject err = new JsonObject();
                err.addProperty("ERROR", "No index given");
                return err;
            }
        }
        return result;
    }
}
