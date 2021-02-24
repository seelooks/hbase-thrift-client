package com.qiku.basec.controller;


import com.qiku.basec.service.HbaseReadService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * @Description:
 * @Auther: v-wangbo-os@360os.com
 * @Date: 2021/2/20 16:14
 */
@RestController
public class HbaseReadController {

    @Autowired
    HbaseReadService hbaseReadService;

    @GetMapping("/all/table")
    public List<String> hbaseTableNames(){
        return hbaseReadService.findHbaseTables();
    }


    @GetMapping( value = "/cell/{tableName}")
    public List<String> hbaseTableCellNames(@PathVariable("tableName") String tableName){
        return hbaseReadService.findHbaseTablesCellName(tableName);
    }



    @GetMapping( value = "/data/{tableName}/{size}")
    public List<Map<String,String>> queryHbaseTableData(@PathVariable("tableName") String tableName,@PathVariable("size") Integer size){
       if (size == null){
           size = 1000;
       }
        return hbaseReadService.queryHbaseTableData(tableName,size);
    }





}