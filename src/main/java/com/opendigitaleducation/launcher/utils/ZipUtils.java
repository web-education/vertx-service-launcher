package com.opendigitaleducation.launcher.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.Promise;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class ZipUtils {
    private static final Logger log = LoggerFactory.getLogger(ZipUtils.class);

    public static void unzip(Vertx vertx, String input, String output, Handler<AsyncResult<Void>> handler) {
        if (input.endsWith(".jar")) {
            unzipJar(vertx, input, output, handler);
        } else if (input.endsWith(".tar.gz")) {
            ungzip(vertx, input, output, true, handler);
        } else {
            ungzip(vertx, input, output, false, handler);
        }
    }

    public static void ungzip(Vertx vertx, String input, String output, Boolean gzip,
            Handler<AsyncResult<Void>> handler) {
        vertx.executeBlocking(promise -> {
            final long start = System.currentTimeMillis();
            TarArchiveInputStream tar = null;
            try {
                tar = new TarArchiveInputStream(!gzip ? new FileInputStream(new File(input))
                        : new GzipCompressorInputStream(new FileInputStream(new File(input))));
                TarArchiveEntry entry;
                while ((entry = tar.getNextTarEntry()) != null) {
                    final File f = new File(output + File.separator + entry.getName());
                    if (entry.isDirectory()) {
                        f.mkdirs();
                        continue;
                    }
                    if (!f.getParentFile().exists()) {
                        f.getParentFile().mkdirs();
                    }
                    FileOutputStream fos = null;
                    try {
                        fos = new FileOutputStream(f);
                        IOUtils.copy(tar, fos);
                    } catch (Exception e) {
                        log.error("Error while unzip tar.gz entry : " + entry.getName() + " -> " + input, e);
                        if (!promise.future().isComplete()) {
                            promise.fail(e);
                        }
                    } finally {
                        if (fos != null) {
                            fos.close();
                        }
                    }
                }
            } catch (IOException e) {
                if (!promise.future().isComplete()) {
                    promise.fail(e);
                    log.error("Error while unzip tar.gz : " + input, e);
                }
            } finally {
                if (tar != null) {
                    try {
                        tar.close();
                    } catch (IOException e) {
                        log.error("Error closing tar file." + input, e);
                    }
                }
            }
            log.info(input + " - uncompress duration : " + (System.currentTimeMillis() - start));
            if (!promise.future().isComplete()) {
                promise.complete();
            }
        }, handler);
    }

    public static void unzipJar(Vertx vertx, String jarFile, String destDir, Handler<AsyncResult<Void>> handler) {
        vertx.executeBlocking(promise -> {
            final long start = System.currentTimeMillis();
            JarFile jar = null;
            try {
                jar = new JarFile(jarFile);
                Enumeration<JarEntry> enumEntries = jar.entries();
                while (enumEntries.hasMoreElements()) {
                    final JarEntry file = (JarEntry) enumEntries.nextElement();
                    final File f = new File(destDir + File.separator + file.getName());
                    if (file.isDirectory()) {
                        f.mkdirs();
                        continue;
                    }
                    if (!f.getParentFile().exists()) {
                        f.getParentFile().mkdirs();
                    }
                    InputStream is = null;
                    FileOutputStream fos = null;
                    try {
                        is = jar.getInputStream(file);
                        fos = new FileOutputStream(f);
                        IOUtils.copy(is, fos);
                    } catch (Exception e) {
                        log.error("Error while unzip jar entry : " + file.getName() + " -> " + jarFile, e);
                        if (!promise.future().isComplete()) {
                            promise.fail(e);
                        }
                    } finally {
                        if (fos != null) {
                            fos.close();
                        }
                        if (is != null) {
                            is.close();
                        }
                    }
                }
            } catch (IOException e) {
                if (!promise.future().isComplete()) {
                    promise.fail(e);
                    log.error("Error while unzip jar.", e);
                }
            } finally {
                if (jar != null) {
                    try {
                        jar.close();
                    } catch (IOException e) {
                        log.error("Error closing jar file.", e);
                    }
                }
            }
            log.info(jarFile + " - uncompress duration : " + (System.currentTimeMillis() - start));
            if (!promise.future().isComplete()) {
                promise.complete();
            }
        }, handler);
    }
}