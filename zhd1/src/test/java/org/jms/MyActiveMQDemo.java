package org.jms;  
  
/** 
  java通过ActiveMQ实现JMS的消息队列实例 
 下载安装http://apache.fayea.com/activemq/ 
 启动命令：bin\win64\activemq.bat 
 http://www.cnblogs.com/phoebus0501/archive/2011/02/24/1964228.html 
 */  
public class MyActiveMQDemo {  
    //http://blog.sina.com.cn/s/blog_a459dcf501017oml.html需要安装ActiveMQ 然后启动bin\win64\activemq.bat  
  
    public static void main(String[] args) {  
        String url = "tcp://192.168.119.5:61616";  
        String user = "henry";  
        String password = "123";  
        String query = "MyQueue";  
        new Thread(new MessageReceiver(query,url,user,password), "Name-Receiver").start();  
        new Thread(new MessageSender(query,url,user,password), "Name-Sender").start();  
    }  
} 