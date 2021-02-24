package com.qiku.basec.service;

import com.qiku.basec.constant.HbaseConfigConstant;
import com.qiku.basec.util.HbaseReadUtil;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * @Description:
 * @Auther: v-wangbo-os@360os.com
 * @Date: 2021/2/24 15:14
 */
@SuppressWarnings("all")
@Service
public class HbaseReadService {

    public List<String> findHbaseTables(){
        TTransport transport = null;
        List<ByteBuffer> tables = null;
        try {
            TTransport trans = new TSocket(HbaseConfigConstant.HOST, HbaseConfigConstant.PORT);
            transport = new TFramedTransport(trans);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            Hbase.Iface handler = new Hbase.Client(protocol);
            tables = handler.getTableNames();
        } catch (TException e) {
            e.printStackTrace();
        }finally {
            close(transport);
        }
        return HbaseReadUtil.byteBuffersToStrs(tables);
    }


    public List<String> findHbaseTablesCellName(String tableName){
        TTransport transport = null;
        try {
            TTransport trans = new TSocket(HbaseConfigConstant.HOST, HbaseConfigConstant.PORT);
            transport = new TFramedTransport(trans);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            Hbase.Iface handler = new Hbase.Client(protocol);
            Map<ByteBuffer, ByteBuffer> map = new HashMap<>();
            ByteBuffer table = ByteBuffer.wrap(tableName.getBytes());
            org.apache.hadoop.hbase.thrift.generated.TScan tScan = new org.apache.hadoop.hbase.thrift.generated.TScan();
            int resId = handler.scannerOpenWithScan(table,tScan,map);
            List<TRowResult> rowResultList = handler.scannerGet(resId);
            if (rowResultList == null || rowResultList.isEmpty()){
                return new ArrayList<String>();
            }
            TRowResult rowResult = rowResultList.get(0);
            Map<ByteBuffer, TCell> mapTCell =  rowResult.getColumns();
            Set<ByteBuffer> mapTCellKeySet = mapTCell.keySet();
            List<ByteBuffer> mapTCellKeyList = new ArrayList<>(mapTCellKeySet);
            return HbaseReadUtil.byteBuffersToStrs(mapTCellKeyList);
        } catch (TException e) {
            e.printStackTrace();
        }finally {
            close(transport);
        }
        return new ArrayList<String>();
    }


    public List<Map<String,String>> queryHbaseTableData(String tableName,int size){
        TTransport transport = null;
        List<Map<String,String>> dataMapList = new ArrayList<>();
        try {
            TTransport trans = new TSocket(HbaseConfigConstant.HOST, HbaseConfigConstant.PORT);
            transport = new TFramedTransport(trans);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            Hbase.Iface handler = new Hbase.Client(protocol);
            Map<ByteBuffer, ByteBuffer> map = new HashMap<>();
            ByteBuffer table = ByteBuffer.wrap(tableName.getBytes());
            org.apache.hadoop.hbase.thrift.generated.TScan tScan = new org.apache.hadoop.hbase.thrift.generated.TScan();
            int resId = handler.scannerOpenWithScan(table,tScan,map);

            List<TRowResult>  rowResultList = handler.scannerGetList(resId,size);
            if (rowResultList == null || rowResultList.isEmpty()){
                return null;
            }
            for(TRowResult rowResultItem:rowResultList){
                Map<String,String> dataMap = new HashMap<>();
                Map<ByteBuffer, TCell>  mapTcell = rowResultItem.getColumns();
                Set<Map.Entry<ByteBuffer, TCell>> setEntity =  mapTcell.entrySet();
                for (Map.Entry<ByteBuffer, TCell> item : setEntity){
                    String key = HbaseReadUtil.byteBufferToStr(item.getKey());
                    TCell cell =   item.getValue();
                    dataMap.put(key,new String(cell.getValue()));
                }
                dataMapList.add(dataMap);
            }
            return dataMapList;
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            close(transport);
        }

        return null;
    }





    private void close(TTransport transport) {
        if (transport != null) {
            transport.close();
        }
    }





}