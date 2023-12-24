package io.sustc.command;

import io.sustc.dto.AuthInfo;
import io.sustc.service.RecommenderService;
import lombok.val;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.util.List;

@ShellComponent
@ConditionalOnBean(RecommenderService.class)
public class RecommenderCommand {

    @Autowired
    private RecommenderService recommenderService;

<<<<<<< HEAD
    @ShellMethod("rec general")
=======
    @ShellMethod(key = "rec general")
>>>>>>> upstream/main
    public List<String> generalRecommendations(
            @ShellOption(defaultValue = "1") Integer pageSize,
            @ShellOption(defaultValue = "10") Integer pageNum
    ) {
        return recommenderService.generalRecommendations(pageSize, pageNum);
    }

<<<<<<< HEAD
    @ShellMethod("rec user")
=======
    @ShellMethod(key = "rec user")
>>>>>>> upstream/main
    public List<String> recommendVideosForUser(
            @ShellOption(defaultValue = ShellOption.NULL) Long mid,
            @ShellOption(defaultValue = ShellOption.NULL) String pwd,
            @ShellOption(defaultValue = ShellOption.NULL) String qq,
            @ShellOption(defaultValue = ShellOption.NULL) String wechat,
            @ShellOption(defaultValue = "1") Integer pageSize,
            @ShellOption(defaultValue = "10") Integer pageNum
    ) {
        val auth = AuthInfo.builder()
                .mid(mid)
                .password(pwd)
                .qq(qq)
                .wechat(wechat)
                .build();

        return recommenderService.recommendVideosForUser(auth, pageSize, pageNum);
    }

<<<<<<< HEAD
    @ShellMethod("rec video")
=======
    @ShellMethod(key = "rec video")
>>>>>>> upstream/main
    public List<String> recommendNextVideo(String bv) {
        return recommenderService.recommendNextVideo(bv);
    }

<<<<<<< HEAD
    @ShellMethod("rec friends")
=======
    @ShellMethod(key = "rec friends")
>>>>>>> upstream/main
    public List<Long> recommendFriends(
            @ShellOption(defaultValue = ShellOption.NULL) Long mid,
            @ShellOption(defaultValue = ShellOption.NULL) String pwd,
            @ShellOption(defaultValue = ShellOption.NULL) String qq,
            @ShellOption(defaultValue = ShellOption.NULL) String wechat,
            @ShellOption(defaultValue = "1") Integer pageSize,
            @ShellOption(defaultValue = "10") Integer pageNum
    ) {
        val auth = AuthInfo.builder()
                .mid(mid)
                .password(pwd)
                .qq(qq)
                .wechat(wechat)
                .build();

        return recommenderService.recommendFriends(auth, pageSize, pageNum);
    }
}
