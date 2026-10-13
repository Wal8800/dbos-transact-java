package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.EnqueueOptions;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.QueueName;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EnqueueInTransactionTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  @AutoClose DBOSClient client;
  TxEnqueueService txEnqueueService;

  @BeforeEach
  void beforeEach() throws SQLException {
    dataSource = pgContainer.dataSource();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS tx_enqueue_orders");
      stmt.execute("CREATE TABLE tx_enqueue_orders (id TEXT PRIMARY KEY)");
    }

    dbos = new DBOS(pgContainer.dbosConfig().withAppVersion("v1.0.0"));
    var impl = new ClientServiceImpl(dbos);
    impl.setProxy(dbos.registerProxy(ClientService.class, impl));
    txEnqueueService =
        dbos.registerProxy(TxEnqueueService.class, new TxEnqueueServiceImpl(dbos, dataSource));
    dbos.launch();
    dbos.registerQueue("testQueue", new QueueOptions());

    client = pgContainer.dbosClient();
  }

  static EnqueueOptions options() {
    return new EnqueueOptions("enqueueTest", "ClientServiceImpl", QueueName.of("testQueue"));
  }

  private Connection begin() throws SQLException {
    var conn = dataSource.getConnection();
    conn.setAutoCommit(false);
    return conn;
  }

  private static void insertOrder(Connection conn, String id) throws SQLException {
    try (var stmt = conn.prepareStatement("INSERT INTO tx_enqueue_orders (id) VALUES (?)")) {
      stmt.setString(1, id);
      stmt.executeUpdate();
    }
  }

  private int countOrders(String id) throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement("SELECT count(*) FROM tx_enqueue_orders WHERE id = ?")) {
      stmt.setString(1, id);
      try (var rs = stmt.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  @Test
  public void commitEnqueuesWithCallersWrite() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    WorkflowHandle<String, RuntimeException> handle;
    try (var conn = begin()) {
      // A statement first, so the enqueue joins a transaction already under way.
      insertOrder(conn, "order-1");
      handle =
          client.enqueueWorkflowInTransaction(
              conn, options().withWorkflowId(workflowId), new Object[] {42, "spam"});
      assertEquals(workflowId, handle.workflowId());

      assertNull(DBUtils.getWorkflowRow(dataSource, workflowId));
      assertEquals(0, countOrders("order-1"));

      conn.commit();
    }
    assertEquals(1, countOrders("order-1"));
    assertEquals("42-spam", handle.getResult());
  }

  @Test
  public void rollbackDiscardsWorkflow() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    try (var conn = begin()) {
      insertOrder(conn, "order-1");
      client.enqueueWorkflowInTransaction(
          conn, options().withWorkflowId(workflowId), new Object[] {42, "spam"});
      conn.rollback();
    }
    assertNull(DBUtils.getWorkflowRow(dataSource, workflowId));
    assertEquals(0, countOrders("order-1"));
  }

  @Test
  public void delayIsHonoured() throws Exception {
    var delay = Duration.ofSeconds(60);
    var workflowId = UUID.randomUUID().toString();
    long before = System.currentTimeMillis();
    try (var conn = begin()) {
      client.enqueueWorkflowInTransaction(
          conn, options().withWorkflowId(workflowId).withDelay(delay), new Object[] {42, "spam"});
      conn.commit();
    }
    long after = System.currentTimeMillis();

    var row = DBUtils.getWorkflowRow(dataSource, workflowId);
    assertEquals(WorkflowState.DELAYED.name(), row.status());
    assertTrue(row.delayUntilEpochMs() >= before + delay.toMillis());
    assertTrue(row.delayUntilEpochMs() <= after + delay.toMillis());
  }

  @Test
  public void repeatedWorkflowIdInOneTransaction() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    WorkflowHandle<String, RuntimeException> second;
    try (var conn = begin()) {
      client.enqueueWorkflowInTransaction(
          conn, options().withWorkflowId(workflowId), new Object[] {42, "spam"});
      second =
          client.enqueueWorkflowInTransaction(
              conn, options().withWorkflowId(workflowId), new Object[] {17, "eggs"});
      conn.commit();
    }
    assertEquals(workflowId, second.workflowId());
    assertEquals("42-spam", second.getResult());
  }

  @Test
  public void deduplicationClashAbortsTransaction() throws Exception {
    DBOSTestAccess.getQueueService(dbos).pause();
    var held =
        client.enqueueWorkflow(options().withDeduplicationId("dedup-1"), new Object[] {1, "held"});

    try (var conn = begin()) {
      insertOrder(conn, "order-1");
      var e =
          assertThrows(
              DBOSQueueDuplicatedException.class,
              () ->
                  client.enqueueWorkflowInTransaction(
                      conn, options().withDeduplicationId("dedup-1"), new Object[] {2, "clash"}));
      assertEquals("dedup-1", e.deduplicationId());

      var aborted = assertThrows(SQLException.class, () -> insertOrder(conn, "order-2"));
      assertEquals("25P02", aborted.getSQLState());
      conn.rollback();
    }
    assertEquals(0, countOrders("order-1"));
    assertEquals(
        WorkflowState.ENQUEUED.name(),
        DBUtils.getWorkflowRow(dataSource, held.workflowId()).status());
  }

  @Test
  public void autocommitConnectionIsRefused() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    try (var conn = dataSource.getConnection()) {
      assertTrue(conn.getAutoCommit());
      assertThrows(
          IllegalArgumentException.class,
          () ->
              client.enqueueWorkflowInTransaction(
                  conn, options().withWorkflowId(workflowId), new Object[] {42, "spam"}));
    }
    assertNull(DBUtils.getWorkflowRow(dataSource, workflowId));
  }

  @Test
  public void dbosEnqueuesInTransaction() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    WorkflowHandle<String, RuntimeException> handle;
    try (var conn = begin()) {
      insertOrder(conn, "order-1");
      handle =
          dbos.enqueueWorkflowInTransaction(
              conn, options().withWorkflowId(workflowId), new Object[] {42, "spam"});
      assertNull(DBUtils.getWorkflowRow(dataSource, workflowId));
      conn.commit();
    }
    assertEquals(1, countOrders("order-1"));
    assertEquals("42-spam", handle.getResult());
  }

  @Test
  public void dbosRefusesInsideWorkflow() throws Exception {
    var message = txEnqueueService.enqueueInTransaction();
    assertTrue(message.contains("inside a workflow"), message);
    assertTrue(
        DBUtils.getWorkflowRows(dataSource).stream()
            .noneMatch(row -> "enqueueTest".equals(row.workflowName())));
  }
}

interface TxEnqueueService {
  String enqueueInTransaction();
}

class TxEnqueueServiceImpl implements TxEnqueueService {

  private final DBOS dbos;
  private final DataSource dataSource;

  TxEnqueueServiceImpl(DBOS dbos, DataSource dataSource) {
    this.dbos = dbos;
    this.dataSource = dataSource;
  }

  @Workflow
  public String enqueueInTransaction() {
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      dbos.enqueueWorkflowInTransaction(
          conn, EnqueueInTransactionTest.options(), new Object[] {42, "spam"});
      conn.commit();
      return "enqueued";
    } catch (IllegalStateException e) {
      return e.getMessage();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
