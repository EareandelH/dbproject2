package io.sustc.benchmark;

<<<<<<< HEAD
import lombok.Builder;
import lombok.Data;

=======
import lombok.Data;

import java.util.concurrent.atomic.AtomicLong;

>>>>>>> upstream/main
/**
 * Evaluation result of a benchmark task.
 * If any of the fields is null, it means the tasks won't be evaluated by this term.
 */
@Data
<<<<<<< HEAD
@Builder
=======
>>>>>>> upstream/main
public class BenchmarkResult {

    private Integer id;

<<<<<<< HEAD
    private Long caseCnt;

    private Long passCnt;

    private Long elapsedTime;
=======
    private Long passCnt;

    private Long elapsedTime;

    public BenchmarkResult(Long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public BenchmarkResult(Long passCnt, Long elapsedTime) {
        this.passCnt = passCnt;
        this.elapsedTime = elapsedTime;
    }

    public BenchmarkResult(AtomicLong passCnt, Long elapsedTime) {
        this(passCnt.get(), elapsedTime);
    }
>>>>>>> upstream/main
}
