package io.sustc.command;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.util.List;

@ShellComponent
@ConditionalOnBean(DanmuService.class)
public class DanmuCommand {

    @Autowired
    private DanmuService danmuService;

<<<<<<< HEAD
    @ShellMethod("danmu send")
=======
    @ShellMethod(key = "danmu send")
>>>>>>> upstream/main
    public long sendDanmu(
            @ShellOption(defaultValue = ShellOption.NULL) Long mid,
            @ShellOption(defaultValue = ShellOption.NULL) String pwd,
            @ShellOption(defaultValue = ShellOption.NULL) String qq,
            @ShellOption(defaultValue = ShellOption.NULL) String wechat,
            String bv,
            @ShellOption(defaultValue = "") String content,
            Float time
    ) {
        val auth = AuthInfo.builder()
                .mid(mid)
                .password(pwd)
                .qq(qq)
                .wechat(wechat)
                .build();

        return danmuService.sendDanmu(auth, bv, content, time);
    }

<<<<<<< HEAD
    @ShellMethod("danmu display")
=======
    @ShellMethod(key = "danmu display")
>>>>>>> upstream/main
    public List<Long> displayDanmu(
            String bv,
            Float timeStart,
            Float timeEnd,
            @ShellOption(defaultValue = "false") Boolean filter
    ) {
        return danmuService.displayDanmu(bv, timeStart, timeEnd, filter);
    }

<<<<<<< HEAD
    @ShellMethod("danmu like")
=======
    @ShellMethod(key = "danmu like")
>>>>>>> upstream/main
    public boolean likeDanmu(
            @ShellOption(defaultValue = ShellOption.NULL) Long mid,
            @ShellOption(defaultValue = ShellOption.NULL) String pwd,
            @ShellOption(defaultValue = ShellOption.NULL) String qq,
            @ShellOption(defaultValue = ShellOption.NULL) String wechat,
            Long id
    ) {
        val auth = AuthInfo.builder()
                .mid(mid)
                .password(pwd)
                .qq(qq)
                .wechat(wechat)
                .build();

        return danmuService.likeDanmu(auth, id);
    }
}
