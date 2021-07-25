#### Week05  作业：Hadoop RPC

根据文档中的示例，完成一个类似的 RPC 函数，要求：

- 输入你的真实学号，返回你的真实姓名
- 输入学号 20210000000000，返回 null
- 输入学号 20210123456789，返回心心

**须知：**文档请于网页版“我的教室”中下载，APP 版暂时不支持附件~
**作业上交：**截图 server 端和 client 端的执行结果 String findName(int studentId)
**作业提交链接：** [ https://jinshuju.net/f/AlDgyT](https://jinshuju.net/f/AlDgyT)



1. ServiceProtocol:

   ```
   public interface ServiceProtocol extends VersionedProtocol{
   
   	public static final long versionID = 1L;
   	
   	String findName(String studentId);
   	
   	int add (int v1, int v2);
   }
   ```

   

2. ServiceProtocolImpl

   ```
   public class ServiceProtocolImpl implements ServiceProtocol{
   
   	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
   
   		return ServiceProtocol.versionID;
   	}
   
   	public String findName(String studentId) {
   		System.out.println("ServiceProtocolImpl findName studentId="+studentId);
   
   		if ("20210579020278".equals(studentId)) {
   			return "yinhuihui";
   		}else {
   			return null;
   		}
   
   	}
   
   }
   
   ```

   

3. RpcServer 代码：

   ```
   public class RpcServer {
   	
   	public final static int PORT = 8899;
   	
   	
   	public static void main(String[] args) {
   		RPC.Builder builder = new RPC.Builder(new Configuration());
   		
   		//bind ip
   		builder.setBindAddress("127.0.0.1");
   		//set port
   		builder.setPort(RpcServer.PORT);
   		
   		builder.setProtocol(ServiceProtocol.class);
   		builder.setInstance(new ServiceProtocolImpl());
   		
   		try {
   			
   			RPC.Server server = builder.build();
   			server.start();
   			System.out.println("Hadoop Server started at:" + "127.0.0.1" +":" + RpcServer.PORT);
   		}catch(Exception e) {
   			e.printStackTrace();
   		}
   	}
   
   }
   ```

   

4. RpcClient代码

   ```
   public class RpcClient {
   
   	static InetSocketAddress addr = new InetSocketAddress("127.0.0.1", RpcServer.PORT);
   	
   	static void printName(String studentId, String studentName) {
   		if (null== studentName) {
   			System.out.println("no such student, studentId=" + studentId);
   		}else {
   			System.out.println("Yes, studentId=" + studentId +" name=" + studentName);
   		}
   		
   	}
   	public static void main(String[] args) throws Exception {
   		ServiceProtocol proxy = RPC.getProxy(ServiceProtocol.class, ServiceProtocol.versionID, addr, new Configuration());
   		
   		String studentId ="1111122";
   		System.out.println("call ServiceProtocol: findName,studentId= " + studentId);
   		
   		String studentName = proxy.findName(studentId);
   		printName(studentId, studentName);
   		
   		studentId ="20210579020278";
   		System.out.println("call ServiceProtocol: findName,studentId= " + studentId);		
   		studentName = proxy.findName(studentId);
   		printName(studentId, studentName);
   	}
   
   }
   ```



执行结果

1. Server执行结果：

   ![](https://github.com/changanjennifer/Bigdata/blob/main/week05/hadoopRpc-Server.png)

2.  Client 执行结果：

![](https://github.com/changanjennifer/Bigdata/blob/main/week05/hadoopRpc-client-result.png)

