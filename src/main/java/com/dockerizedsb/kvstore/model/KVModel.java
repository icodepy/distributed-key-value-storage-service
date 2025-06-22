package com.dockerizedsb.kvstore.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KVModel {
    private String key;
    private String value;
}
