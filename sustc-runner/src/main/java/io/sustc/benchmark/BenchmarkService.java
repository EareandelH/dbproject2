package io.sustc.benchmark;

<<<<<<< HEAD
import io.fury.Fury;
import io.fury.ThreadSafeFury;
import io.fury.config.CompatibleMode;
import io.fury.config.Language;
import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
=======
import io.fury.ThreadSafeFury;
import io.sustc.dto.*;
import io.sustc.service.*;
>>>>>>> upstream/main
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

<<<<<<< HEAD
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.List;
=======
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
>>>>>>> upstream/main

@Service
@Slf4j
public class BenchmarkService {

    @Autowired
<<<<<<< HEAD
    private BenchmarkConfig benchmarkConfig;
=======
    private BenchmarkConfig config;
>>>>>>> upstream/main

    @Autowired
    private DatabaseService databaseService;

<<<<<<< HEAD
    @BenchmarkStep(order = 1, timeout = 10, description = "Import data")
    @SneakyThrows
    public BenchmarkResult importData() {
        val dataDir = Paths.get(benchmarkConfig.getDataPath(), BenchmarkConstants.IMPORT_DATA_PATH);

        ThreadSafeFury fury = Fury.builder()
                .requireClassRegistration(false)
                .withLanguage(Language.JAVA)
                .withRefTracking(true)
                .withCompatibleMode(CompatibleMode.COMPATIBLE)
                .withAsyncCompilation(true)
                .buildThreadSafeFury();

        FileInputStream danmuStream = new FileInputStream(
                dataDir.resolve(BenchmarkConstants.DANMU_FILENAME).toString());
        byte[] danmuBytes = danmuStream.readAllBytes();
        danmuStream.close();
        List<DanmuRecord> danmuRecords = (List<DanmuRecord>) fury.deserialize(danmuBytes);

        FileInputStream userSteram = new FileInputStream(dataDir.resolve(BenchmarkConstants.USER_FILENAME).toString());
        byte[] userBytes = userSteram.readAllBytes();
        userSteram.close();
        List<UserRecord> userRecords = (List<UserRecord>) fury.deserialize(userBytes);

        FileInputStream videoStream = new FileInputStream(
                dataDir.resolve(BenchmarkConstants.VIDEO_FILENAME).toString());
        byte[] videoBytes = videoStream.readAllBytes();
        videoStream.close();
        List<VideoRecord> videoRecords = (List<VideoRecord>) fury.deserialize(videoBytes);

        val startedTime = System.nanoTime();
=======
    @Autowired(required = false)
    private DanmuService danmuService;

    @Autowired(required = false)
    private RecommenderService recommenderService;

    @Autowired(required = false)
    private UserService userService;

    @Autowired(required = false)
    private VideoService videoService;

    @Autowired
    private ThreadSafeFury fury;

    private final Map<Long, String> sentDanmu = new ConcurrentHashMap<>();

    private final Set<String> postedVideo = new ConcurrentSkipListSet<>();

    private final Set<Long> registeredUser = new ConcurrentSkipListSet<>();

    @BenchmarkStep(order = 0, description = "Truncate tables")
    public void truncate() {
        if (!config.isStudentMode()) {
            return;
        }
        log.warn("Truncating tables");
        databaseService.truncate();
    }

    @BenchmarkStep(order = 1, timeout = 35, description = "Import data")
    public BenchmarkResult importData() {
        List<DanmuRecord> danmuRecords = deserialize(BenchmarkConstants.IMPORT_DATA, BenchmarkConstants.DANMU_RECORDS);
        List<UserRecord> userRecords = deserialize(BenchmarkConstants.IMPORT_DATA, BenchmarkConstants.USER_RECORDS);
        List<VideoRecord> videoRecords = deserialize(BenchmarkConstants.IMPORT_DATA, BenchmarkConstants.VIDEO_RECORDS);

        val startTime = System.currentTimeMillis();
>>>>>>> upstream/main
        try {
            databaseService.importData(danmuRecords, userRecords, videoRecords);
        } catch (Exception e) {
            log.error("Exception encountered during importing data, you may early stop this run", e);
        }
<<<<<<< HEAD
        val finishedTime = System.nanoTime();

        return BenchmarkResult.builder()
                .elapsedTime(finishedTime - startedTime)
                .build();
=======
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(endTime - startTime);
    }

    @BenchmarkStep(order = 2, description = "Test VideoService#searchVideo(AuthInfo, String, int, int)")
    public BenchmarkResult videoSearch1() {
        List<Map.Entry<Object[], List<String>>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.VIDEO_SEARCH_1);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.parallelStream().forEach(it -> {
            try {
                val args = it.getKey();
                val res = videoService.searchVideo((AuthInfo) args[0], (String) args[1], (int) args[2], (int) args[3]);
                if (collectionEquals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 3, description = "Test VideoService#getAverageViewRate(String)")
    public BenchmarkResult videoViewRate() {
        Map<String, Double> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.VIDEO_VIEW_RATE);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.entrySet().parallelStream().forEach(it -> {
            try {
                val res = videoService.getAverageViewRate(it.getKey());
                if (Math.abs(it.getValue() - res) < BenchmarkConstants.EPS) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 4, description = "Test VideoService#getHotspot(String)")
    public BenchmarkResult videoHotspot() {
        Map<String, Set<Integer>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.VIDEO_HOTSPOT);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.entrySet().parallelStream().forEach(it -> {
            try {
                val res = videoService.getHotspot(it.getKey());
                if (collectionEquals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 5, timeout = 8, description = "Test RecommenderService#recommendNextVideo(String)")
    public BenchmarkResult recVideo() {
        Map<String, List<String>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.REC_VIDEO);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.entrySet().parallelStream().forEach(it -> {
            try {
                val res = recommenderService.recommendNextVideo(it.getKey());
                if (collectionEquals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 6, timeout = 8, description = "Test RecommenderService#generalRecommendations(int, int)")
    public BenchmarkResult recGeneral() {
        List<Map.Entry<int[], List<String>>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.REC_GENERAL);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.parallelStream().forEach(it -> {
            try {
                val args = it.getKey();
                val res = recommenderService.generalRecommendations(args[0], args[1]);
                if (collectionEquals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 7, timeout = 8, description = "Test RecommenderService#recommendVideosForUser(AuthInfo, int, int)")
    public BenchmarkResult recUser() {
        List<Map.Entry<Object[], List<String>>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.REC_USER);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.parallelStream().forEach(it -> {
            try {
                val args = it.getKey();
                val res = recommenderService.recommendVideosForUser((AuthInfo) args[0], (int) args[1], (int) args[2]);
                if (collectionEquals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 8, timeout = 8, description = "Test RecommenderService#recommendFriends(AuthInfo, int, int)")
    public BenchmarkResult recFriends() {
        List<Map.Entry<Object[], List<Long>>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.REC_FRIENDS);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.parallelStream().forEach(it -> {
            try {
                val args = it.getKey();
                val res = recommenderService.recommendFriends((AuthInfo) args[0], (int) args[1], (int) args[2]);
                if (collectionEquals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 9, description = "Test DanmuService#displayDanmu(String, float, float, boolean)")
    public BenchmarkResult danmuDisplay() {
        List<Map.Entry<Object[], Integer>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.DANMU_DISPLAY);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.parallelStream().forEach(it -> {
            try {
                val args = it.getKey();
                val res = danmuService.displayDanmu((String) args[0], (float) args[1], (float) args[2], (boolean) args[3]);
                val resSize = Objects.isNull(res) ? 0 : res.size();
                if (it.getValue() == resSize) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected size {}, got {}", it.getKey(), it.getValue(), resSize);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 10, description = "Test DanmuService#sendDanmu(AuthInfo, String, String, float)")
    public BenchmarkResult danmuSend() {
        List<Map.Entry<Object[], Boolean>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.DANMU_SEND);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.parallelStream().forEach(it -> {
            try {
                val args = it.getKey();
                val res = danmuService.sendDanmu((AuthInfo) args[0], (String) args[1], (String) args[2], (float) args[3]);
                if (Boolean.TRUE.equals(it.getValue())) {
                    if (res >= 0) {
                        sentDanmu.put(res, (String) args[1]);
                        pass.incrementAndGet();
                    } else {
                        log.debug("Wrong answer for {}: expected >= 0, got {}", it.getKey(), res);
                    }
                } else {
                    if (res < 0) {
                        pass.incrementAndGet();
                    } else {
                        log.debug("Wrong answer for {}: expected < 0, got {}", it.getKey(), res);
                    }
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 11, description = "Test UserService#getUserInfo(long)")
    public BenchmarkResult getUserInfo() {
        Map<Long, UserInfoResp> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.USER_INFO);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.entrySet().parallelStream().forEach(it -> {
            try {
                val res = userService.getUserInfo(it.getKey());
                if (userInfoEquals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it.getKey(), e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 12, description = "Test DanmuService#likeDanmu(AuthInfo, long)")
    public BenchmarkResult danmuLike() {
        Map<Long, AuthInfo> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.DANMU_LIKE);
        val pass = new AtomicLong();

        val danmuIDs = new ArrayList<>(sentDanmu.keySet());
        val random = new Random();

        cases.entrySet().parallelStream().forEach(it -> {
            try {
                val danmuId = danmuIDs.get(random.nextInt(danmuIDs.size()));
                val res = danmuService.likeDanmu(it.getValue(), danmuId);
                val danmuBv = sentDanmu.get(danmuId);
                val watched = Arrays.asList(userService.getUserInfo(it.getKey()).getWatched()).contains(danmuBv);
                if (watched == res) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), watched, res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it.getKey(), e);
            }
        });

        return new BenchmarkResult(pass, null);
    }

    @BenchmarkStep(order = 13, description = "Test VideoService#coinVideo(AuthInfo, String)")
    public BenchmarkResult videoCoin() {
        List<Map.Entry<Object[], Boolean>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.VIDEO_COIN);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            try {
                val args = it.getKey();
                val res = videoService.coinVideo((AuthInfo) args[0], (String) args[1]);
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 14, description = "Test VideoService#likeVideo(AuthInfo, String)")
    public BenchmarkResult videoLike() {
        List<Map.Entry<Object[], Boolean>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.VIDEO_LIKE);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            try {
                val args = it.getKey();
                val res = videoService.likeVideo((AuthInfo) args[0], (String) args[1]);
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 15, description = "Test VideoService#collectVideo(AuthInfo, String)")
    public BenchmarkResult videoCollect() {
        List<Map.Entry<Object[], Boolean>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.VIDEO_COLLECT);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            try {
                val args = it.getKey();
                val res = videoService.collectVideo((AuthInfo) args[0], (String) args[1]);
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 16, description = "Test VideoService#postVideo(AuthInfo, PostVideoReq)")
    public BenchmarkResult videoPost() {
        List<Map.Entry<Object[], Boolean>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.VIDEO_POST);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            try {
                val args = it.getKey();
                val res = videoService.postVideo((AuthInfo) args[0], (PostVideoReq) args[1]);
                if (Boolean.TRUE.equals(it.getValue())) {
                    if (Objects.nonNull(res)) {
                        postedVideo.add(res);
                        pass.incrementAndGet();
                    } else {
                        log.debug("Wrong answer for {}: expected not null, got null", it.getKey());
                    }
                } else {
                    if (Objects.isNull(res)) {
                        pass.incrementAndGet();
                    } else {
                        log.debug("Wrong answer for {}: expected null, got {}", it.getKey(), res);
                    }
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 17, description = "Test UserService#register(RegisterUserReq)")
    public BenchmarkResult userRegister() {
        List<Map.Entry<RegisterUserReq, Boolean>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.USER_REGISTER);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            try {
                val args = it.getKey();
                val res = userService.register(args);
                if (Boolean.TRUE.equals(it.getValue())) {
                    if (res >= 0) {
                        registeredUser.add(res);
                        pass.incrementAndGet();
                    } else {
                        log.debug("Wrong answer for {}: expected >= 0, got {}", it.getKey(), res);
                    }
                } else {
                    if (res < 0) {
                        pass.incrementAndGet();
                    } else {
                        log.debug("Wrong answer for {}: expected < 0, got {}", it.getKey(), res);
                    }
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 18, description = "Test VideoService#updateVideoInfo(AuthInfo, String, PostVideoReq)")
    public BenchmarkResult videoUpdate() {
        List<Map.Entry<Object[], Boolean>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.VIDEO_UPDATE);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            try {
                val args = it.getKey();
                val res = videoService.updateVideoInfo((AuthInfo) args[0], (String) args[1], (PostVideoReq) args[2]);
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 19, description = "Test VideoService#reviewVideo(AuthInfo, String)")
    public BenchmarkResult videoReview() {
        List<Map.Entry<Object[], Boolean>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.VIDEO_REVIEW);
        AuthInfo superuser = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.SUPER_USER_AUTH);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            try {
                val args = it.getKey();
                val res = videoService.reviewVideo((AuthInfo) args[0], (String) args[1]);
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        postedVideo.parallelStream().forEach(it -> {
            try {
                val res = videoService.reviewVideo(superuser, it);
                if (res) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected true, got false", it);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 20, description = "Test side effect of step 17, 18")
    public BenchmarkResult videoSearch2() {
        List<Map.Entry<Object[], List<String>>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.VIDEO_SEARCH_2);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.parallelStream().forEach(it -> {
            try {
                val args = it.getKey();
                val res = videoService.searchVideo((AuthInfo) args[0], (String) args[1], (int) args[2], (int) args[3]);
                if (collectionEquals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 21, description = "Test VideoService#deleteVideo(AuthInfo, String)")
    public BenchmarkResult videoDelete() {
        List<Map.Entry<Object[], Boolean>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.VIDEO_DELETE);
        val pass = new AtomicLong();

        AuthInfo superuser = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.SUPER_USER_AUTH);

        val startTime = System.currentTimeMillis();
        postedVideo.parallelStream().forEach(it -> {
            try {
                val res = videoService.deleteVideo(superuser, it);
                if (res) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected true, got false", it);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        cases.forEach(it -> {
            try {
                val args = it.getKey();
                val res = videoService.deleteVideo((AuthInfo) args[0], (String) args[1]);
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 22, description = "Test UserService#deleteAccount(AuthInfo, long)")
    public BenchmarkResult userDelete() {
        List<Map.Entry<Object[], Boolean>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.USER_DELETE);
        val pass = new AtomicLong();

        AuthInfo superuser = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.SUPER_USER_AUTH);

        val startTime = System.currentTimeMillis();
        registeredUser.parallelStream().forEach(it -> {
            try {
                val res = userService.deleteAccount(superuser, it);
                if (res) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected true, got false", it);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        cases.forEach(it -> {
            try {
                val args = it.getKey();
                val res = userService.deleteAccount((AuthInfo) args[0], (long) args[1]);
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it, e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @BenchmarkStep(order = 23, description = "Test UserService#follow(AuthInfo, long)")
    public BenchmarkResult userFollow() {
        List<Map.Entry<Object[], Boolean>> cases = deserialize(BenchmarkConstants.TEST_DATA, BenchmarkConstants.USER_FOLLOW);
        val pass = new AtomicLong();

        val startTime = System.currentTimeMillis();
        cases.forEach(it -> {
            try {
                val args = it.getKey();
                val res = userService.follow((AuthInfo) args[0], (long) args[1]);
                if (Objects.equals(it.getValue(), res)) {
                    pass.incrementAndGet();
                } else {
                    log.debug("Wrong answer for {}: expected {}, got {}", it.getKey(), it.getValue(), res);
                }
            } catch (Exception e) {
                log.error("Exception thrown for {}", it.getKey(), e);
            }
        });
        val endTime = System.currentTimeMillis();

        return new BenchmarkResult(pass, endTime - startTime);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    private <T> T deserialize(String... path) {
        val file = Paths.get(config.getDataPath(), path);
        return (T) fury.deserialize(Files.readAllBytes(file));
    }

    private static boolean collectionEquals(Collection<?> expect, Collection<?> actual) {
        return Objects.equals(expect, actual)
                || expect.isEmpty() && Objects.isNull(actual);
    }

    private static boolean longArrayAsSetEquals(long[] expect, long[] actual) {
        if (expect.length != actual.length) {
            return false;
        }
        val expectSet = new HashSet<Long>();
        for (val i : expect) {
            expectSet.add(i);
        }
        for (val i : actual) {
            if (!expectSet.remove(i)) {
                return false;
            }
        }
        return expectSet.isEmpty();
    }

    private static <T> boolean arrayAsSetEquals(T[] expect, T[] actual) {
        if (expect.length != actual.length) {
            return false;
        }
        return Objects.equals(new HashSet<>(Arrays.asList(expect)), new HashSet<>(Arrays.asList(actual)));
    }

    private static boolean userInfoEquals(UserInfoResp expect, UserInfoResp actual) {
        return expect.getMid() == actual.getMid()
                && expect.getCoin() == actual.getCoin()
                && longArrayAsSetEquals(expect.getFollowing(), actual.getFollowing())
                && longArrayAsSetEquals(expect.getFollower(), actual.getFollower())
                && arrayAsSetEquals(expect.getWatched(), actual.getWatched())
                && arrayAsSetEquals(expect.getLiked(), actual.getLiked())
                && arrayAsSetEquals(expect.getCollected(), actual.getCollected())
                && arrayAsSetEquals(expect.getPosted(), actual.getPosted());
>>>>>>> upstream/main
    }
}
