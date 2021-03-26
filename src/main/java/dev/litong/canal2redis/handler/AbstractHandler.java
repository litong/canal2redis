package dev.litong.canal2redis.handler;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.google.protobuf.InvalidProtocolBufferException;
import dev.litong.canal2redis.util.RedisUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 数据库操作处理基类
 *
 * @author litong
 */
@Slf4j
public abstract class AbstractHandler {

    /**
     * Redis工具实例
     */
    @Autowired
    protected RedisUtil redisUtil;

    /**
     * 下一个执行者
     */
    AbstractHandler nextHandler;

    /**
     * 事件类型
     */
    EventType eventType;

    /**
     * 处理Canal消息
     *
     * @param entry Canal消息
     */
    public void handleMessage(Entry entry) {
        if (this.eventType == entry.getHeader().getEventType()) {
            //发生写入操作的库名
            String database = entry.getHeader().getSchemaName();
            //发生写入操作的表名
            String table = entry.getHeader().getTableName();
            log.info("监听到数据库：{}，表：{} 的 {} 事件", database, table, eventType.toString());

            Optional.ofNullable(this.getRowChange(entry))
                    .ifPresent(rowChange -> handleRowChange(database, table, rowChange));
        } else {
            if (nextHandler != null) {
                nextHandler.handleMessage(entry);
            }
        }
    }

    /**
     * 处理数据库数据
     *
     * @param database  数据库名称
     * @param table     表名称
     * @param rowChange 行更改数据
     */
    public abstract void handleRowChange(String database, String table, RowChange rowChange);

    /**
     * 获得发生操作的数据
     *
     * @param entry Canal消息
     * @return 行更改数据
     */
    private RowChange getRowChange(Entry entry) {
        RowChange rowChange = null;
        try {
            rowChange = RowChange.parseFrom(entry.getStoreValue());
        } catch (InvalidProtocolBufferException e) {
            log.error("根据CanalEntry获取RowChange异常:", e);
        }
        return rowChange;
    }

    /**
     * 行数据转Map数据
     */
    Map<String, String> columnsToMap(List<Column> columns) {
        return columns.stream().collect(Collectors.toMap(Column::getName, Column::getValue));
    }


}
