/*
*  Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.carbon.esb.connector;

import java.nio.ByteBuffer;

import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.identitymanagement.model.CreateRoleRequest;
import com.amazonaws.services.identitymanagement.model.EntityAlreadyExistsException;
import com.amazonaws.services.identitymanagement.model.GetRoleRequest;
import com.amazonaws.services.identitymanagement.model.MalformedPolicyDocumentException;
import com.amazonaws.services.identitymanagement.model.PutRolePolicyRequest;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.DeliveryStreamDescription;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.ListDeliveryStreamsRequest;
import com.amazonaws.services.kinesisfirehose.model.ListDeliveryStreamsResult;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.util.IOUtils;
import com.amazonaws.util.StringUtils;

/**
 * Sample method implementation.
 */
public class FirehoseConnector extends AbstractConnector {
	
	// S3 properties
    protected static AmazonS3Client s3Client;
    protected static String s3BucketName;
    protected static String s3RegionName;

    // DeliveryStream properties
    protected static AmazonKinesisFirehoseClient firehoseClient;
    protected static String accountId;
    protected static String deliveryStreamName;
    protected static String firehoseRegion;

    // S3Destination Properties
    protected static String iamRoleName;
    protected static String s3DestinationAWSKMSKeyId;
    protected static Integer s3DestinationSizeInMBs;
    protected static Integer s3DestinationIntervalInSeconds;
    
    // IAM Role
    protected static String iamRegion;
    protected static AmazonIdentityManagement iamClient;
    
    protected static String data;
    protected static String awsKey;
    protected static String awsSecret;

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        awsKey = (String) getParameter(messageContext, "awsKey");
        awsSecret = (String) getParameter(messageContext, "awsSecret");
        s3BucketName = (String) getParameter(messageContext, "s3BucketName");
        s3RegionName = (String) getParameter(messageContext, "s3RegionName");
        accountId = (String) getParameter(messageContext, "accountId");
        deliveryStreamName = (String) getParameter(messageContext, "deliveryStreamName");
        firehoseRegion = (String) getParameter(messageContext, "firehoseRegion");
        iamRoleName = (String) getParameter(messageContext, "iamRoleName");
        
        try {
            log.info("firehose connector received awsKey :" + awsKey);
            log.info("firehose connector received awsSecret :" + awsSecret);
            log.info("firehose connector received s3BucketName :" + s3BucketName);
            log.info("firehose connector received s3RegionName :" + s3RegionName);
            log.info("firehose connector received accountId :" + accountId);
            log.info("firehose connector received deliveryStreamName :" + deliveryStreamName);
            log.info("firehose connector received firehoseRegion :" + firehoseRegion);
            log.info("firehose connector received iamRoleName :" + iamRoleName);
            data = messageContext.getEnvelope().getBody().getText();
            /**Add your connector code here 
            **/
            initClients();
            log.info("Putting record in deliveryStream : " + deliveryStreamName + " via Put Record method.");
            putRecordIntoDeliveryStream();
            log.info("Done putting record.");
        } catch (Exception e) {
	    throw new ConnectException(e);	
        }
    }
    
    /**
     * Method to initialize the clients using the specified AWSCredentials.
     *
     * @param Exception
     */
    protected static void initClients() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default] credential
         * profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
    	BasicAWSCredentials credentials = new BasicAWSCredentials(awsKey, awsSecret);

        // S3 client
        s3Client = new AmazonS3Client(credentials);
        Region s3Region = RegionUtils.getRegion(s3RegionName);
        s3Client.setRegion(s3Region);

        // Firehose client
        firehoseClient = new AmazonKinesisFirehoseClient(credentials);
        firehoseClient.setRegion(RegionUtils.getRegion(firehoseRegion));

        // IAM client
        iamClient = new AmazonIdentityManagementClient(credentials);
        iamClient.setRegion(RegionUtils.getRegion(iamRegion));
    }
    
    /**
     * Method to put records in the specified delivery stream by reading
     * contents from sample input file using PutRecord API.
     *
     * @throws IOException
     */
    protected static void putRecordIntoDeliveryStream() throws Exception {
        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setDeliveryStreamName(deliveryStreamName);

        Record record = createRecord(data);
        putRecordRequest.setRecord(record);

        // Put record into the DeliveryStream
        firehoseClient.putRecord(putRecordRequest);
    }
    
    /**
     * Method to create the record object for given data.
     *
     * @param data the content data
     * @return the Record object
     */
    private static Record createRecord(String data) {
        return new Record().withData(ByteBuffer.wrap(data.getBytes()));
    }
}