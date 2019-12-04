package com.lavapm.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Access the Hdfs with kerberos.
 * Created by dalei on 10/5/19.
 */
public class HdfsKerberos {

    private FileSystem hdfs;

    public HdfsKerberos(String host, String port) {
        this.hdfs = getFileSystem(host, port);
    }


    /**
     * kerbers认证调用方式一
     *
     * @param config
     */
    private static void kerberosConfig1(Configuration config) {

        String krb5File = "/etc/krb5.conf";
        String kerUser = "dmp_app_user/cdh-master.lavapm@LAVAPM.COM";
        String keyPath = "/home/dmp_app_user/dmp_app_user.keytab";

        System.setProperty("java.security.krb5.conf", krb5File);
        config.set("hadoop.security.authentication", "kerberos");

        UserGroupInformation.setConfiguration(config);
        try {
            UserGroupInformation.loginUserFromKeytab(kerUser, keyPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * kerbers认证调用方式二
     *
     * @param config
     */
    private static void kerberosConfig2(Configuration config) {

        String krb5File = "/etc/krb5.conf";
        String kerUser = "dmp_app_user/cdh-master.lavapm@LAVAPM.COM";
        String keyPath = "/home/dmp_app_user/dmp_app_user.keytab";

        System.setProperty("java.security.krb5.conf", krb5File);
        System.setProperty("java.security.krb5.conf", krb5File);

        String coreSite = "/etc/hadoop/conf/core-site.xml";
        String hdfsSite = "/etc/hadoop/conf/hdfs-site.xml";

        config.addResource(new Path(coreSite));
        config.addResource(new Path(hdfsSite));

        UserGroupInformation.setConfiguration(config);

        try {
            UserGroupInformation.loginUserFromKeytab(kerUser, keyPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @return 得到hdfs的连接 FileSystem类
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public static FileSystem getFileSystem(String host, String port) {

        // 获取FileSystem类的方法有很多种，这里只写一种
        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        config.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");

        /** 推荐方式一*/
        kerberosConfig1(config);
//        kerberosConfig2(config);


        try {
            // hdfs连接地址
            URI uri = new URI("hdfs://" + host + ":" + port);
            // 第一位为uri，第二位为config，第三位是登录的用户
            //方式一
            return FileSystem.get(uri, config);
            //方式二
//            return FileSystem.get(config);
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 删除文件或者文件夹
     *
     * @param src
     * @throws Exception
     */
    public void delete(String src) throws Exception {
        Path p1 = new Path(src);
        if (hdfs.isDirectory(p1)) {
            hdfs.delete(p1, true);
            System.out.println("删除文件夹成功: " + src);
        } else if (hdfs.isFile(p1)) {
            hdfs.delete(p1, false);
            System.out.println("删除文件成功: " + src);
        }
    }

    /**
     * 将一个字符串写入某个路径
     *
     * @param text 要保存的字符串
     * @param path 要保存的路径
     */
    public void writerString(String text, String path) {

        try {
            Path f = new Path(path);
            FSDataOutputStream os = hdfs.create(f, true);
            // 以UTF-8格式写入文件，不乱码
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "utf-8"));
            writer.write(text);
            writer.close();
            os.close();
        } catch (Exception e) {
            e.printStackTrace();

        }

    }

    /**
     * 读取文件目录列表
     *
     * @param dir
     * @return
     */
    public List<String> readDirList(String dir) {
        if (StringUtils.isBlank(dir)) {
            return null;
        }
        if (!dir.startsWith("/")) {
//            dir = dir.substring(1);
            dir = "/" + dir;
        }
        List<String> result = new ArrayList<String>();
        FileStatus[] stats = null;
        try {
            stats = hdfs.listStatus(new Path(dir));
            for (FileStatus file : stats) {
                if (file.isFile() && file.getLen() != 0) {
                    result.add(file.getPath().toUri().getPath());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * @param data
     * @param path
     */
    public void append(String data, String path) {
        try {
            Path f = new Path(path);
            if (!hdfs.exists(f)) {
                hdfs.createNewFile(f);
            }
            FSDataOutputStream os = hdfs.append(new Path(path));
            // 以UTF-8格式写入文件，不乱码
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "utf-8"));
            writer.write(data);
            writer.write("\n");
            writer.close();
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 转换文件编码为UTF-8，上传至hdfs服务器
     *
     * @param data
     * @param path
     * @author jipengfei
     */
    public void uploadFileToHdfs(String data, String path) {
        try {
            data = new String(data.getBytes(), "utf-8");
            this.writerString(data, path);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            HdfsKerberos hdfsKerberos = new HdfsKerberos("cdh-master.lavapm", "8020");
//            hdfsKerberos.uploadFileToHdfs("111111","/user/dp/file/data/temp/11111.csv");
//            hdfsKerberos.append("2222","/user/dmp_app_user/tmp//11111.csv");
//            hdfsKerberos.delete("/user/dmp_app_user/tmp/11111.csv");
            List<String> list = hdfsKerberos.readDirList("/user/dmp_app_user/tmp/");
            System.out.println("==============================================");
            System.out.println(list);
            System.out.println("==============================================");
        } catch (Exception e) {
            e.printStackTrace();
        }

//        String hdfsPath = "/user/dp/file/data/jiajunfang/datasource/353/1532331191608.CSV";
//        String localPath = "F:";
//        hdfsUtil.downloadFileFromHdfs(hdfsPath, localPath);
    }
}


