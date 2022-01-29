package com.example.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.util.CollectionUtils;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CanalClient {

    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {
        //TODO 创建连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("192.168.227.129", 11111),
                "example", "", "");
        while (true) {
            //TODO 连接
            canalConnector.connect();
            //TODO 订阅数据库
            canalConnector.subscribe("student");
            //TODO 获取数据
            Message message = canalConnector.get(100);
            //TODO 获取entry集合
            List<CanalEntry.Entry> entries = message.getEntries();
            if (!CollectionUtils.isEmpty(entries)) {
                //TODO 遍历entry单条解析
                for (CanalEntry.Entry entry : entries) {
                    //todo 1.表名
                    String tableName = entry.getHeader().getTableName();
                    //todo 2.获取类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    //todo 3.获取序列化后的数据
                    ByteString storeValue = entry.getStoreValue();
                    //todo 4.判断当前entryType类型是否为rowdata
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        System.out.println(entryType);
                        //todo 5.反序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //todo 6.获取当前事件的操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //todo 7.获取数据集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        for (CanalEntry.RowData rowData : rowDatasList) {
                            JSONObject before = new JSONObject();
                            JSONObject after = new JSONObject();
                            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                            for (CanalEntry.Column column : beforeColumnsList) {
                                before.put(column.getName(), column.getValue());
                            }
                            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                            for (CanalEntry.Column column : afterColumnsList) {
                                after.put(column.getName(), column.getValue());
                            }
                            System.out.println("表名：" + tableName + ",EntryType：" + entryType + ",事件的操作类型:" + eventType + ",before:" + before + ",after:" + after);
                        }
                    } else {
                        System.out.println("当前类型为：" + entryType);
                    }
                }
            } else {
                System.out.println("暂无数据...");
                TimeUnit.SECONDS.sleep(3);
            }
        }

    }

}
