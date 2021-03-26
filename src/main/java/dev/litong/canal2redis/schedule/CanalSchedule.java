package dev.litong.canal2redis.schedule;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.Message;
import dev.litong.canal2redis.handler.InsertHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Canal定时任务
 * @author litong
 */
@Slf4j
@Component
public class CanalSchedule {

    private final CanalConnector canalConnector;

    private final InsertHandler insertHandler;

    @Value("${canal.batchSize}")
    private int batchSize;

    @Autowired
    public CanalSchedule(CanalConnector canalConnector, InsertHandler insertHandler) {
        this.canalConnector = canalConnector;
        this.insertHandler = insertHandler;
    }

    @Async("canal")
    @Scheduled(fixedDelay = 200)
    public void fetch() {
        try {
            Message message = canalConnector.getWithoutAck(batchSize);
            long batchId = message.getId();
            log.debug("batchId={}", batchId);
            try {
                List<Entry> entries = message.getEntries();
                if (batchId != -1 && entries.size() > 0) {
                    entries.forEach(entry -> {
                        if (entry.getEntryType() == EntryType.ROWDATA) {
                            insertHandler.handleMessage(entry);
                        }
                    });
                }
                canalConnector.ack(batchId);
            } catch (Exception e) {
                log.error("批量获取 mysql 同步信息失败，batchId回滚,batchId=" + batchId, e);
                canalConnector.rollback(batchId);
            }
        } catch (Exception e) {
            log.error("Canal定时任务异常！", e);
        }
    }
}
