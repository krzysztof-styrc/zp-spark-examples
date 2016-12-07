package com.codewise.spark.examples;

import com.codewise.sdl.binary.BinaryObjectReader;
import com.google.common.base.Objects;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.input.PortableDataStream;
import pl.zeropark.common.model.sdl.zeroclick.ZeroclickRequest;
import pl.zeropark.common.model.sdl.zeroclick.ZeroclickRequestPacket;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

public class ZpRequestsCountExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Zeropark requests count");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Broadcast<String> abc = sc.broadcast("abc");

        JavaPairRDD<String, PortableDataStream> rdd = sc
                .binaryFiles(
                        "s3n://zeropark-test-visit-packets2/zeroclick_request/2016/12/01/00/0021dd07-2db7-44af-958b" +
                                "-9543dda36ddb.lz4",
                        1);
        JavaRDD<RequestData> xxx = rdd
                .flatMap(t ->
                        decodeZeroclickRequestPacket(t._2())
                                .getItems().stream().map(RequestData::new)
                                .iterator())
                .sortBy(RequestData::getCity, true, 4);

        xxx.filter(..)
                .map(RequestData::toString)
                .saveAsTextFile("s3n://zp-tarzan-boys/sorted_requests/requests2.txt");

        xxx.groupBy()

        xxx
                .map(RequestData::toString)
                .saveAsTextFile("s3n://zp-tarzan-boys/sorted_requests/requests.txt");

        sc.stop();
    }
    private static ZeroclickRequestPacket decodeZeroclickRequestPacket(PortableDataStream portableDataStream)
            throws IOException {
        LZ4FastDecompressor LZ4_DECOMPRESSOR = LZ4Factory.fastestInstance().fastDecompressor();

        DataInputStream stream = portableDataStream.open();
        byte[] compressed = IOUtils.toByteArray(stream);
        byte[] sdlBinaryData = LZ4_DECOMPRESSOR.decompress(compressed, 66783); // TODO: read this value from S3 Metadata
        DataInput input = new DataInputStream(new ByteArrayInputStream(sdlBinaryData));
        BinaryObjectReader reader = new BinaryObjectReader(input);
        return ZeroclickRequestPacket.read(reader);
    }

    static class RequestData implements Serializable {
        private String region;
        private String city;

        public RequestData(ZeroclickRequest request) {
            this.region = request.getRegion();
            this.city = request.getCity();
        }

        public String getRegion() {
            return region;
        }

        public void setRegion(String region) {
            this.region = region;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("region", region)
                    .add("city", city)
                    .toString();
        }
    }
}