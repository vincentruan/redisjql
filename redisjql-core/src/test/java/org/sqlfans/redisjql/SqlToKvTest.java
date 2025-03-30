package org.sqlfans.redisjql;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.sqlfans.redisjql.cache.CacheOperationService;
import org.sqlfans.redisjql.cache.caffine.CaffeineCacheOperationService;
import org.sqlfans.redisjql.config.IndexConfig;
import org.sqlfans.redisjql.parser.SelectParser;
import org.sqlfans.redisjql.parser.impl.SelectParserImpl;
import org.sqlfans.redisjql.parser.InsertParser;
import org.sqlfans.redisjql.parser.impl.InsertParserImpl;
import org.sqlfans.redisjql.parser.UpdateParser;
import org.sqlfans.redisjql.parser.impl.UpdateParserImpl;
import org.sqlfans.redisjql.sync.DataSyncService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class SqlToKvTest {
    
    private CacheOperationService redisOperationService;
    
    @Mock
    private JdbcTemplate jdbcTemplate;
    
    private List<IndexConfig> indexConfigs;
    private SelectParser selectParser;
    private InsertParser insertParser;
    private UpdateParser updateParser;
    private DataSyncService dataSyncService;

    @Before
    public void setup() {
        // 初始化CaffeineCacheOperationService
        redisOperationService = new CaffeineCacheOperationService();
        
        // 配置索引
        indexConfigs = new ArrayList<>();
        IndexConfig indexConfig = new IndexConfig();
        indexConfig.setTableName("tm_case_main");
        indexConfig.setPrimaryKey("case_no");
        indexConfig.setVersionField("jpa_version");
        
        // 添加普通索引
        IndexConfig.IndexDefinition nameIndex = new IndexConfig.IndexDefinition();
        nameIndex.setName("name_idx");
        nameIndex.addField("name");
        nameIndex.setSortField("create_time");
        indexConfig.addIndex(nameIndex);
        
        // 添加复合索引
        IndexConfig.IndexDefinition statusTimeIndex = new IndexConfig.IndexDefinition();
        statusTimeIndex.setName("status_time_idx");
        statusTimeIndex.addField("status");
        statusTimeIndex.addField("create_time");
        statusTimeIndex.setSortField("create_time");
        statusTimeIndex.setUnique(false);
        indexConfig.addIndex(statusTimeIndex);
        
        // 添加唯一索引
        IndexConfig.IndexDefinition codeIndex = new IndexConfig.IndexDefinition();
        codeIndex.setName("code_idx");
        codeIndex.addField("case_code");
        codeIndex.setUnique(true);
        indexConfig.addIndex(codeIndex);
        
        indexConfigs.add(indexConfig);
        
        // 初始化解析器
        selectParser = new SelectParserImpl(redisOperationService, indexConfigs);
        insertParser = new InsertParserImpl(redisOperationService, indexConfigs);
        updateParser = new UpdateParserImpl(redisOperationService, indexConfigs);
        
        // 初始化同步服务
        dataSyncService = new DataSyncService(jdbcTemplate, redisOperationService, indexConfigs);
        
        // 预先添加一些测试数据到缓存
        String indexKey = "tm_case_main:name:Test Case";
        redisOperationService.addIndexRecord(indexKey, "CASE001", 0);
        redisOperationService.addIndexRecord(indexKey, "CASE002", 0);
        redisOperationService.addIndexRecord(indexKey, "CASE003", 0);
        
        // 添加复合查询的测试数据
        String statusIndexKey = "tm_case_main:status:OPEN";
        redisOperationService.addIndexRecord(statusIndexKey, "CASE001", 1674259200000L); // 2023-01-21
        redisOperationService.addIndexRecord(statusIndexKey, "CASE002", 1674345600000L); // 2023-01-22
        redisOperationService.addIndexRecord(statusIndexKey, "CASE003", 1674432000000L); // 2023-01-23
        
        // 为了支持复合查询，添加记录的create_time属性数据
        redisOperationService.addDataField("tm_case_main:data:CASE001", "create_time", "2023-01-21");
        redisOperationService.addDataField("tm_case_main:data:CASE002", "create_time", "2023-01-22");
        redisOperationService.addDataField("tm_case_main:data:CASE003", "create_time", "2023-01-23");
    }

    @Test
    public void testInsert() throws JSQLParserException {
        // 测试插入数据
        String insertSql = "INSERT INTO tm_case_main (case_no, name, status, create_time, case_code, jpa_version) " +
                            "VALUES ('CASE004', 'Test Case', 'OPEN', '2023-01-01', 'TC001', 1)";
        
        Statement statement = CCJSqlParserUtil.parse(insertSql);
        assertTrue("应该解析为Insert语句", statement instanceof Insert);
        
        Insert insertStatement = (Insert) statement;
        Integer result = insertParser.parse(insertStatement);
        
        // 由于使用了Mock，这里只能验证解析过程，不能验证实际的Redis操作
        assertNotNull("解析结果不应为空", result);
    }

    @Test
    public void testSelect() throws JSQLParserException {
        // 测试单一条件查询
        String selectSql = "SELECT * FROM tm_case_main WHERE name = 'Test Case'";
        
        Statement statement = CCJSqlParserUtil.parse(selectSql);
        assertTrue("应该解析为Select语句", statement instanceof Select);
        
        Select selectStatement = (Select) statement;
        Object result = selectParser.parse(selectStatement);
        
        assertNotNull("查询结果不应为空", result);
        
        // 测试复合条件查询
        String complexSelectSql = "SELECT * FROM tm_case_main WHERE status = 'OPEN' AND create_time > '2023-01-01' ORDER BY create_time DESC";
        
        statement = CCJSqlParserUtil.parse(complexSelectSql);
        assertTrue("应该解析为Select语句", statement instanceof Select);
        
        selectStatement = (Select) statement;
        result = selectParser.parse(selectStatement);
        
        assertNotNull("复合查询结果不应为空", result);
    }

    @Test
    public void testUpdate() throws JSQLParserException {
        // 测试更新数据 - 通过主键更新
        String updateSql = "UPDATE tm_case_main SET name = 'Updated Case', status = 'CLOSED', jpa_version = jpa_version + 1 " +
                            "WHERE case_no = 'CASE001' AND jpa_version = 1";
        
        Statement statement = CCJSqlParserUtil.parse(updateSql);
        assertTrue("应该解析为Update语句", statement instanceof Update);
        
        Update updateStatement = (Update) statement;
        Integer result = updateParser.parse(updateStatement);
        
        assertNotNull("更新结果不应为空", result);
        
        // 测试更新数据 - 通过索引字段更新
        String updateByIndexSql = "UPDATE tm_case_main SET name = 'Index Updated Case', jpa_version = jpa_version + 1 " +
                                "WHERE case_code = 'TC001' AND jpa_version = 1";
        
        statement = CCJSqlParserUtil.parse(updateByIndexSql);
        assertTrue("应该解析为Update语句", statement instanceof Update);
        
        updateStatement = (Update) statement;
        result = updateParser.parse(updateStatement);
        
        assertNotNull("通过索引更新结果不应为空", result);
    }

    @Test
    public void testSelectRewrite() throws JSQLParserException {
        // 测试查询SQL改写
        String selectSql = "SELECT * FROM tm_case_main WHERE name = 'Test Case' ORDER BY create_time";
        
        // Statement statement = CCJSqlParserUtil.parse(selectSql);
        // Select selectStatement = (Select) statement;
        
        // 模拟获取主键列表
        Set<String> primaryKeys = new HashSet<>(Arrays.asList("CASE001", "CASE002", "CASE003"));
        
        // 改写SQL
        String rewrittenSql = selectParser.rewriteSelectSql(selectSql, new ArrayList<>(primaryKeys));
        
        assertNotNull("改写后的SQL不应为空", rewrittenSql);
        assertTrue("改写后的SQL应包含IN条件", rewrittenSql.contains("IN"));
        assertTrue("改写后的SQL应包含主键值", rewrittenSql.contains("CASE001"));
    }

    @Test
    public void testDataSync() {
        // 测试数据同步服务
        // 设置同步间隔
        dataSyncService.setSyncIntervalMinutes(5);
        
        // 由于是模拟测试，这里只能验证不会抛出异常
        try {
            dataSyncService.start();
            dataSyncService.stop();
            assertTrue(true);
        } catch (Exception e) {
            fail("数据同步服务启动或停止时不应抛出异常");
        }
    }

    @Test
    public void testVersionField() throws JSQLParserException {
        // 测试版本字段检查
        String insertWithoutVersionSql = "INSERT INTO tm_case_main (case_no, name, status) " +
                                        "VALUES ('CASE005', 'No Version Case', 'OPEN')";
        
        Statement statement = CCJSqlParserUtil.parse(insertWithoutVersionSql);
        Insert insertStatement = (Insert) statement;
        
        // 没有版本字段的情况下，应该不会执行Redis操作
        Integer result = insertParser.parse(insertStatement);
        assertEquals("没有版本字段应返回0", Integer.valueOf(0), result);
    }

    @Test
    public void testUniqueIndexQuery() throws JSQLParserException {
        // 测试唯一索引查询
        String selectByUniqueSql = "SELECT * FROM tm_case_main WHERE case_code = 'TC001'";
        
        Statement statement = CCJSqlParserUtil.parse(selectByUniqueSql);
        Select selectStatement = (Select) statement;
        
        // 唯一索引查询应直接走数据库，不走Redis缓存
        assertFalse("唯一索引查询不应使用Redis缓存", selectParser.canUseRedisCache(selectStatement));
    }
}
