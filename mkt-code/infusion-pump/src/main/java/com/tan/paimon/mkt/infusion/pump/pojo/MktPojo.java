package com.tan.paimon.mkt.infusion.pump.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class MktPojo {

    // table name
    private String tableName;

    // tags
    private String host;
    private Integer port;
    private String factoryNum;
    private String equType;
    private String equNum;

    // values
    private Long ts;
    private Float presetValue;
    private Float speed;
    private Integer alreadyInjectTime;
    private Integer remainTime;
    private Float alreadyInjectValue;
    private Float residual;
    private Float pressureValue;
    private String pressureUint;

    private String workSta;
    private String drugName;
    private String injectMode;

    private String deptName;
    private String roomNo;
    private String bedNo;
}
