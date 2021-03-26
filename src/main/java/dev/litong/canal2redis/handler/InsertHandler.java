package dev.litong.canal2redis.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 插入操作处理类
 *
 * @author litong
 */
@Slf4j
@Component
public class InsertHandler extends AbstractHandler {

    public InsertHandler() {
        this.eventType = EventType.INSERT;
    }

    @Autowired
    public void setNextHandler(DeleteHandler deleteHandler) {
        this.nextHandler = deleteHandler;
    }

    @Override
    public void handleRowChange(String database, String table, RowChange rowChange) {
        rowChange.getRowDatasList().forEach(rowData -> {
            // 新增的数据
            Map<String, String> afterMap = super.columnsToMap(rowData.getAfterColumnsList());
            String jsonStr = JSONObject.toJSONString(afterMap);
            log.info("新增的数据：{}\r\n", jsonStr);

            String id = afterMap.get("id");
            redisUtil.setDefault(database + ":" + table + ":" + id, jsonStr);
        });
    }


}
