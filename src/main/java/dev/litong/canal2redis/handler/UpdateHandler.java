package dev.litong.canal2redis.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 更新操作处理类
 *
 * @author litong
 */
@Slf4j
@Component
public class UpdateHandler extends AbstractHandler {

    public UpdateHandler() {
        this.eventType = EventType.UPDATE;
    }

    @Override
    public void handleRowChange(String database, String table, RowChange rowChange) {
        rowChange.getRowDatasList().forEach(rowData -> {
            // 更新前数据
            Map<String, String> beforeMap = super.columnsToMap(rowData.getBeforeColumnsList());
            String beforeJsonStr = JSONObject.toJSONString(beforeMap);
            log.info("更新前数据：{}", beforeJsonStr);

            // 更新后数据
            Map<String, String> afterMap = super.columnsToMap(rowData.getAfterColumnsList());
            String afterJsonStr = JSONObject.toJSONString(afterMap);
            log.info("更新后数据：{}\r\n", afterJsonStr);

            /*
             * 高并发下，为保证数据一致性，当数据库更新后，不建议去更新缓存，
             * 而是建议直接删除缓存，由查询时再设置到缓存。
             * 这里为了演示，对缓存作更新操作，具体看业务需求。
             */
            String id = afterMap.get("id");
            redisUtil.setDefault(database + ":" + table + ":" + id, afterJsonStr);

        });
    }
}
