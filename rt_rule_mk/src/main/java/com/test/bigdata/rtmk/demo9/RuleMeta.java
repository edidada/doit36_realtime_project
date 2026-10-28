package com.test.bigdata.rtmk.demo9;

import com.test.bigdata.rtmk.demo5.RuleCalculator;
import lombok.*;

import java.time.LocalDateTime;

@Data
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class RuleMeta {
    // 本字段，代表cdc从mysql监听过来的数据的rowKind（+I,-U,+U,-D)
    private int opType;
    private int id;
    private int model_id;

    private String model_calculator_code;

    private String param_json;
    private byte[] static_crowd;
    private int status;
    private String publisher;
    private LocalDateTime create_time;
    private LocalDateTime update_time;

    private RuleCalculator ruleCalculator;
}
