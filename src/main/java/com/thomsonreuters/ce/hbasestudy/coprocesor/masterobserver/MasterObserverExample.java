package com.thomsonreuters.ce.hbasestudy.coprocesor.masterobserver;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;


public class MasterObserverExample extends BaseMasterObserver {
	@Override
	public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> env,
			 HTableDescriptor desc,
             HRegionInfo[] regions)
					throws IOException {
		String tableName = desc.getNameAsString();
		MasterServices services = env.getEnvironment().getMasterServices();
		MasterFileSystem masterFileSystem = services.getMasterFileSystem();
		FileSystem fileSystem = masterFileSystem.getFileSystem();
		System.out.println("++++++++++++++++++++++++++++++++++");
		System.out.println("fileSystem.getCanonicalServiceName()="+fileSystem.getCanonicalServiceName());
		System.out.println("++++++++++++++++++++++++++++++++++");
		Path blobPath = new Path(tableName + "-blobs");
		fileSystem.mkdirs(blobPath);
		env.bypass();
		
	}
}