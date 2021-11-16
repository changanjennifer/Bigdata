public class HbaseTest {

	/**
	 * 配置
	 */
	static Configuration config = null;
	private Connection connection = null;
	private Table table = null;

	@Before
	public void init() throws Exception {
		config = HBaseConfiguration.create();// 配置
		config.set("hbase.zookeeper.quorum", "192.168.30.66");// zookeeper地址
		config.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
		connection = ConnectionFactory.createConnection(config);
		table = connection.getTable(TableName.valueOf("student"));
	}

	/**
	 * 创建数据库表student，并增加列族info和score
	 *
	 * @throws Exception
	 */
	@Test
	public void createTable() throws Exception {
		// 创建表管理类
		HBaseAdmin admin = new HBaseAdmin(config); // hbase表管理
		// 创建表描述类
		TableName tableName = TableName.valueOf("student"); // 表名称 student
		HTableDescriptor desc = new HTableDescriptor(tableName);
		// 创建列族的描述类
		HColumnDescriptor familyInfo = new HColumnDescriptor("info"); // 列族 info
		// 将列族添加到表中
		desc.addFamily(familyInfo);
		HColumnDescriptor familyScore = new HColumnDescriptor("score"); // 列族 score
		// 将列族添加到表中
		desc.addFamily(familyScore);
		// 创建表
		admin.createTable(desc); // 创建表
		System.out.println("创建表成功！");
	}

	/**
	 * 插入基础数据
	 * @throws Exception
	 */
	@SuppressWarnings({ "deprecation", "resource" })
	@Test
	public void insertData() throws Exception {
		table.setAutoFlushTo(false);
		table.setWriteBufferSize(534534534);
		ArrayList<Put> arrayList = new ArrayList<Put>();

		Put putTom = new Put(Bytes.toBytes("Tom"));
		putTom.add(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000001"));
		putTom.add(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("1"));
		putTom.add(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("75"));
		putTom.add(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("82"));
		arrayList.add(putTom);

		Put putJerry = new Put(Bytes.toBytes("Jerry"));
		putJerry.add(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000002"));
		putJerry.add(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("1"));
		putJerry.add(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("85"));
		putJerry.add(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("67"));
		arrayList.add(putJerry);

		Put putJack = new Put(Bytes.toBytes("Jack"));
		putJack.add(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000003"));
		putJack.add(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("2"));
		putJack.add(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("80"));
		putJack.add(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("80"));
		arrayList.add(putJack);

		Put putRose = new Put(Bytes.toBytes("Rose"));
		putRose.add(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000004"));
		putRose.add(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("2"));
		putRose.add(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("60"));
		putRose.add(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("61"));
		arrayList.add(putRose);
		
		//插入数据
		table.put(arrayList);
		//提交
		table.flushCommits();
		System.out.println("数据插入成功！");
	}


	/**
	 * 查询所有数据
	 * @throws Exception
	 */
	@Test
	public void scanDataStep1() throws Exception {

		// 创建全表扫描的scan
		Scan scan = new Scan();
		System.out.println("查询到的所有数据如下：");
		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			if (result.getValue(Bytes.toBytes("info"), Bytes.toBytes("student_id")) == null) {
				for (KeyValue kv : result.raw()) {
				     System.out.print(new String(kv.getRow()) + " ");
				     System.out.print(new String(kv.getFamily()) + ":");
				     System.out.print(new String(kv.getQualifier()) + " = ");
				     System.out.print(new String(kv.getValue()));
				}
		}
		}
	}

	/**
	 * 已知姓名，查询该学生的资料
	 * @throws Exception
	 */
	@Test
	public void scanByName(name) throws Exception {
		Get g = new Get(name.getBytes());
		g.addFamily("score".getBytes());
		// 打印结果集
		Result result = table.get(g);
		for (KeyValue kv : result.raw()) {
		     Get g1 = new Get(kv.getValue());
		     Result result1 = table.get(g1);
		     for (KeyValue kv1 : result1.raw()) {
			     System.out.print(new String(kv1.getRow()) + " ");
			     System.out.print(new String(kv1.getFamily()) + ":");
			     System.out.print(new String(kv1.getQualifier()) + " = ");
			     System.out.print(new String(kv1.getValue()));
			}
		}
	}

	/**
	 * 已知姓名，删除该学生的资料
	 * @throws Exception
	 */
	@Test
	public void deleteByName(name) throws Exception {
	    // 创建删除条件对象
	    Delete delete = new Delete(Bytes.toBytes(name));
		
		// 执行删除操作
		table.delete(delete);
	
	}


	@After
	public void close() throws Exception {
		table.close();
		connection.close();
	}

}