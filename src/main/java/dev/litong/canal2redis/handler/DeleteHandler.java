package dev.litong.canal2redis.handler;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 删除操作处理类
 *
 * @author litong
 */
@Slf4j
@Component
public class DeleteHandler extends AbstractHandler {

    public DeleteHandler() {
        eventType = EventType.DELETE;
    }

    @Autowired
    public void setNextHandler(UpdateHandler updateHandler) {
        this.nextHandler = updateHandler;
    }

    @Override
    public void handleRowChange(String database, String table, RowChange rowChange) {
        rowChange.getRowDatasList().forEach(rowData -> {
            rowData.getBeforeColumnsList().forEach(column -> {
                if ("id".equals(column.getName())) {
                    //清除 redis 缓存
                    log.info("清除 Redis 缓存 key={} 成功!\r\n", database + ":" + table + ":" + column.getValue());
                    redisUtil.delete(database + ":" + table + ":" + column.getValue());
                }
            });
        });
    }
}
