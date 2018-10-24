package com.ibeifeng.sparkproject.spark.ad;

import com.google.common.base.Optional;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.*;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.*;
import com.ibeifeng.sparkproject.util.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * 广告点击流量实时统计spark作业
 *
 * @author Administrator
 */
public class AdClickRealTimeStatSpark {
    public static void main(String[] args) {
        //构建Spark Streaming上下文
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("AdClickRealTimeStatSpark");
        /**
         * spark streaming的上下文是构建JavaStreamingContext对象
         * 而不是像之前的JavaSparkContext、SQLContext/HiveContext
         * 传入 的第一个参数，和之前的spark上下文一样，也是SparkConf对象；第二个参数则不一样
         * 第二个参数是spark streaming类型作业比较有特色的一个参数
         * 实时处理batch的interval
         * spark streaming,每隔一小段时间，会去收集一次数据源(kafka)中的数据，做成一个batch
         * 每次都处理一个batch中的数据
         *
         * 通常来说，batch interval,就是指每隔多少时间收集一次数据源中的数据，然后进行处理
         *一遍spark streaming的应用，都设置数秒到数十秒(很少会超过一分钟)
         *
         * 咱们这里项目中，就设置5秒内的数据源接收过来的数据
         *
         */
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        jssc.checkpoint("hdfs:spark1:9000/streaming_checkpoint");

        /**
         * 正式开始进行代码编写
         * 实现咱们需要的实时计算的业务逻辑和功能
         *
         *
         * 创建针对Kafka数据来源输入DStream(离线流，代表一个源源不断的数据来源，抽象)
         * 选用Kafka direct api(很多好处，包括自己内部自适应调整每次接收数据量的特性，等等)
         *
         * 构建Kafka参数map
         * 主要要放置的就是，你要连接的kafka集群的地址(broker集群的地址列表)
         */
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list",
                ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));

        //构建 topic set
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");

        Set<String> topics = new HashSet<>();
        for (String kafkaTopic : kafkaTopicsSplited) {
            topics.add(kafkaTopic);
        }
        /**
         *基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
         * 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
         */

        JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);

        /**
         * 根据动态黑名单进行数据过滤
         *
         */
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream =
                filterByBlacklist(adRealTimeLogDStream);


        /**
         * 生成动态黑名单
         */
        generateDynamicBlacklist(filteredAdRealTimeLogDStream);

        /**
         * 业务功能一:计算广告点击流量实时统计结果(yyyyMMdd_province_city_adId,clickCount)
         * 最粗
         */
        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(
                filteredAdRealTimeLogDStream);


        /**
         * 业务功能二:实时统计每天每个省份top10热门广告
         * 统计的稍微细一些了
         */
        calculateProvinceTop3Ad(adRealTimeStatDStream);
        /**
         * 业务功能三:实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势(每分钟的点击量)
         * 统计的非常细了
         * 我们每次都可以看到每个广告,最近一小时内的，每分钟的点击量
         * 每支广告的点击趋势
         */
        calculateAdClickCountByWindow(adRealTimeLogDStream);


        /**
         * 构建完spark streaming上下文后，记得要进行上下文的启动，等待执行结束，关闭
         */
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

    /*
     * 计算最近1小时滑动窗口内的广告点击趋势
     * */
    private static void calculateAdClickCountByWindow(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        //映射成<yyyyMMddHHmm_adid,1L>格式
        JavaPairDStream<String, Long> pairDStream = adRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                String[] logSplited = tuple._2.split(" ");
                // timestamp province city userid adid
                String timeMinute = DateUtils.formatTimeMinute(
                        new Date(Long.valueOf(logSplited[0])));
                long adid = Long.parseLong(logSplited[4]);
                return new Tuple2<>(timeMinute + "_" + adid, 1L);
            }
        });
        /*
         * 过来的每个batch rdd,都会被映射成<yyyyMMddHHMM_adid,1L>的格式
         * 每次出来一个新的batch，都要获取最近1小时内的所有的batch
         * 然后根据key进行reduceByKey操作，统计出来最近一小时内的各分钟各广告的点击数量
         * 1小时滑动窗口内的广告点击趋势
         * 战略/拆线图
         * */
        JavaPairDStream<String, Long> aggrRDD = pairDStream.reduceByKeyAndWindow(new Function2<Long, Long, Long>() {

            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.minutes(60), Durations.seconds(10));

        /*
         * aggrRDD
         * 每次都可以拿到，最近1小时内，各分钟(yyyyMMddHHMM)各广告的点击量
         * 各广告，在最近1小时内，各分钟的点击量
         * */
        aggrRDD.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        List<AdClickTrend> adClickTrends = new ArrayList<AdClickTrend>();
                        while (iterator.hasNext()) {
                            Tuple2<String, Long> tuple = iterator.next();
                            String[] keySplited = tuple._1.split("_");
                            //yyyyMMddHHmm
                            String dateMinute = keySplited[0];
                            long adid = Long.parseLong(keySplited[1]);
                            long clickCount = tuple._2;

                            String date = DateUtils.formatDate(DateUtils.parseDateKey(
                                    dateMinute.substring(0, 8)));
                            String hour = dateMinute.substring(8, 10);
                            String minute = dateMinute.substring(10);


                            AdClickTrend adClickTrend = new AdClickTrend();
                            adClickTrend.setDate(date);
                            adClickTrend.setHour(hour);
                            adClickTrend.setMinute(minute);
                            adClickTrend.setAdid(adid);
                            adClickTrend.setClickCount(clickCount);

                            adClickTrends.add(adClickTrend);

                        }
                        IAdClickTrendDAO adClickTrendDAO = DAOFactory.getAdClickTrendDAO();
                        adClickTrendDAO.updateBatch(adClickTrends);
                    }
                });
                return null;
            }
        });
    }

    /*
     * 计算每天各省份的top3热门广告
     * @param adRealTimeStatDStream
     * */
    private static void calculateProvinceTop3Ad(JavaPairDStream<String, Long> adRealTimeStatDStream) {
        /*
         *  adRealTimeStatDStream
         *  每一个batch rdd,都代表了最新的全量的每天各省份各城市各广告的击量
         * */
        JavaDStream<Row> rowsDStream = adRealTimeStatDStream.transform(new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {
            @Override
            public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {
                /*
                 * 原有的<yyyyMMdd_province_city_adid,clickCount>
                 * 要换成<yyyyMMdd_provice_adid,clickCount>
                 * 计算出每天各省份各广告的点击量
                 * */
                JavaPairRDD<String, Long> mappedRDD = rdd.mapToPair(new PairFunction<Tuple2<String, Long>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, Long> tuple) throws Exception {
                        String[] keySplited = tuple._1.split("_");
                        String date = keySplited[0];
                        String province = keySplited[1];
                        long adid = Long.parseLong(keySplited[3]);
                        long clickCount = tuple._2;
                        String key = date + "_" + province + "_" + adid;
                        return new Tuple2<>(key, adid);
                    }
                });
                /*
                 *   对每天每个省份的广告进行初步统计
                 * */
                JavaPairRDD<String, Long> dailyAdClickCountByProvinceRDD = mappedRDD.reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });

                /*
                 * 将dailyAdClickCountByProvinceRDD转换为DataFrame
                 * 注册为一张临时表
                 * 使用Spark SQL,通过开窗函数，获取到各省份的top3热门广告
                 * */
                JavaRDD<Row> rowsRDD = dailyAdClickCountByProvinceRDD.map(new Function<Tuple2<String, Long>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Long> tuple) throws Exception {
                        String[] keySplited = tuple._1.split("_");
                        String dateKey = keySplited[0];
                        String province = keySplited[1];
                        long adid = Long.parseLong(keySplited[2]);
                        long clickCount = tuple._2;
                        String date = DateUtils.formatDate(DateUtils.parseDateKey(dateKey));
                        return RowFactory.create(date, province, adid, clickCount);
                    }
                });
                //定义表结构
                StructType schema = DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("date", DataTypes.StringType, true),
                        DataTypes.createStructField("province", DataTypes.StringType, true),
                        DataTypes.createStructField("ad_id", DataTypes.StringType, true),
                        DataTypes.createStructField("click_count", DataTypes.StringType, true)
                ));
                HiveContext sqlContext = new HiveContext(rdd.context());
                DataFrame dailyAdClickCountByProvinceDF = sqlContext.createDataFrame(rowsRDD, schema);

                //将dailyAdClickCountByProvinceDF注册成一张临时表
                dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov");
                // 使用Spark SQL执行SQL语句，配合开窗函数，统计出各身份top3热门的广告
                DataFrame provinceTop3AdDF = sqlContext.sql(
                        "SELECT "
                                + "date,"
                                + "province,"
                                + "ad_id,"
                                + "click_count "
                                + "FROM ( "
                                + "SELECT "
                                + "date,"
                                + "province,"
                                + "ad_id,"
                                + "click_count,"
                                + "ROW_NUMBER() OVER(PARTITION BY province ORDER BY click_count DESC) rank "
                                + "FROM tmp_daily_ad_click_count_by_prov "
                                + ") t "
                                + "WHERE rank>=3"
                );
                return provinceTop3AdDF.javaRDD();

            }
        });
        /*
         * rowsDStream
         * 每次都刷新出来各个省份最热门的top3广告
         * 将其中的数据批量更新到MySQL中
         * */
        rowsDStream.foreachRDD(new Function<JavaRDD<Row>, Void>() {
            @Override
            public Void call(JavaRDD<Row> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
                    @Override
                    public void call(Iterator<Row> iterator) throws Exception {
                        List<AdProvinceTop3> adProvinceTop3s = new ArrayList<AdProvinceTop3>();
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            String date = row.getString(0);
                            String province = row.getString(1);
                            long adid = row.getLong(2);
                            long clickCount = row.getLong(3);

                            AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
                            adProvinceTop3.setDate(date);
                            adProvinceTop3.setProvince(province);
                            adProvinceTop3.setAdid(adid);
                            adProvinceTop3.setClickCount(clickCount);

                            adProvinceTop3s.add(adProvinceTop3);
                        }
                        IAdProvinceTop3DAO adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
                        adProvinceTop3DAO.updateBatch(adProvinceTop3s);
                    }
                });
                return null;
            }
        });
    }

    /**
     * 计算广告点击流量实时统计
     *
     * @param filteredAdRealTimeLogDStream
     * @return
     */
    private static JavaPairDStream<String, Long> calculateRealTimeStat(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        /*
         * 业务逻辑一
         * 广告点击流量实时统计
         * 上面的黑名单实际上是广告类的实时系统中，比较常见的一种基础的应用
         * 实际上，我们要实现的业务功能，不是黑名单
         *
         * 计算每天各省各城市广告的点击量
         * 这份数据，实时不断地更新到mysql中的，J2EE系统，是提供实时报表给用户查看的
         * j2ee系统每隔几秒名，就从mysql中搂一次最新数据，每次都可能不一样
         * 设计出来几个维度:日期，省份，城市，广告
         * j2ee系统就可以非常的灵活
         * */
        JavaPairDStream<String, Long> mappedDStream = filteredAdRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                String[] logSplited = tuple._2.split(" ");

                String timestamp = logSplited[0];
                Date date = new Date(Long.parseLong(timestamp));
                String datekey = DateUtils.formatDateKey(date);
                String province = logSplited[1];
                String city = logSplited[2];
                long adid = Long.parseLong(logSplited[3]);

                String key = datekey + "_" + province + "_" + city + "_" + adid;
                return new Tuple2<>(key, 1L);
            }
        });

        /*
         * 在这个dstream中，就相当于，有每个batch rdd累加的各个key（各天各省份各城市各广告的点击次数）
         * 每次计算出最新的值，就在aggregatedDStream中的每个batch rdd中反应出来
         * */

        JavaPairDStream<String, Long> aggregatedDStream = mappedDStream.updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
            @Override
            public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {

                /*
                 * 举例来说
                 * 对于每个key，都会调用一次
                 * */
                long clickCount = 0L;
                if (optional.isPresent()) {
                    clickCount = optional.get();
                }
                for (Long value : values) {
                    clickCount += value;
                }
                return Optional.of(clickCount);
            }
        });
        //将计算出来的最新结果，同步一份到mysql中，以便于j2ee系统使用
        aggregatedDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        List<AdStat> adStats = new ArrayList<AdStat>();
                        while (iterator.hasNext()) {
                            Tuple2<String, Long> tuple = iterator.next();
                            String[] keySplited = tuple._1.split("_");
                            String date = keySplited[0];
                            String province = keySplited[1];
                            String city = keySplited[2];
                            long adid = Long.valueOf(keySplited[3]);

                            long clickCount = tuple._2;

                            AdStat adStat = new AdStat();
                            adStat.setDate(date);
                            adStat.setProvince(province);
                            adStat.setCity(city);
                            adStat.setAdid(adid);
                            adStat.setClickCount(clickCount);

                            adStats.add(adStat);

                        }
                        IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
                        adStatDAO.updateBatch(adStats);
                    }
                });
                return null;
            }
        });

        return aggregatedDStream;
    }

    /**
     * 生成动态黑名单
     *
     * @param filteredAdRealTimeLogDStream
     */
    private static void generateDynamicBlacklist(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        /*
         * 一条一条的实时日志
         * timestamp       province      city     userid     adid
         * 某个时间点       某个省份       某个城市 某个用户   某个广告
         *
         * 计算出每5个秒内的数据中，每天每个用户每个广告的点击量
         *
         * 通过对原始实时日志的处理
         * 将日志的格式处理成<yyyyMMdd_userid_adid,1L>格式
         * */
        JavaPairDStream<String, Long> dailyUserAdClickDStream = filteredAdRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                //从tuple中获取到每个原始的实时日志
                String[] logSplited = tuple._2.split(" ");
                // 提取出日期(yyyyMMdd)、userid、adid
                String timestamp = logSplited[0];
                Date date = new Date(Long.parseLong(timestamp));
                String datekey = DateUtils.formatDateKey(date);

                long userid = Long.parseLong(logSplited[3]);
                long adid = Long.parseLong(logSplited[4]);
                String key = datekey + "_" + userid + "_" + adid;
                return new Tuple2<>(key, 1L);
            }
        });
        /*
        * 针对处理后的日志格式，执行reduceByKey算子即可
        * (每个batch中)每天每个用户对每个广告的点击量
        * */
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        /*
        * 到这里为止，获取到了什么数据呢？
        * dailyUserAdClickCountDStream DStream
        * 源源不断的，每个5s的batch中，当天每个用户对每支广告的点击次数
        * <yyyyMMdd_userid_adid,clickCount>
        * */
        dailyUserAdClickCountDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        /*
                        * 对每个分区的数据就去获取一次连接对象
                        * 每次都从连接池中获取，而不是每次都创建
                        * 写数据库操作，性能已经提到最高了
                        * */
                        List<AdUserClickCount> adUserClickCounts = new ArrayList<AdUserClickCount>();
                        while (iterator.hasNext()) {
                            Tuple2<String, Long> tuple =  iterator.next();

                            String[] keySplited = tuple._1.split("_");
                            String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
                            // yyyy-MM-dd
                            long userid = Long.valueOf(keySplited[1]);
                            long adid = Long.valueOf(keySplited[2]);
                            long clickCount = tuple._2;

                            AdUserClickCount adUserClickCount = new AdUserClickCount();
                            adUserClickCount.setDate(date);
                            adUserClickCount.setUserid(userid);
                            adUserClickCount.setAdid(adid);
                            adUserClickCount.setClickCount(clickCount);

                            adUserClickCounts.add(adUserClickCount);
                        }

                        IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                        adUserClickCountDAO.updateBatch(adUserClickCounts);
                    }
                });
                /*
                * 现在我们在mysql里面，已经有了累计的每天各用户对各广告的点击量
                * 遍历每个batch中所有记录，对每条记录都要去查询一下，这一天这个用户对这个广告的累计点击量是多少
                *
                * 从mysql中查询
                * 查询出来的结果，如果是100，如果你发现某个用户某天对某个广告的点击量已经大于等于100
                * 那么就判定这个用户就是黑名单用户，就写入mysql的表中，持久化
                *
                *
                * */
                JavaPairDStream<String, Long> blacklistDStream = dailyUserAdClickCountDStream.filter(new Function<Tuple2<String, Long>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Long> tuple) throws Exception {
                        String key = tuple._1;
                        String[] keySplited = key.split("_");

                        // yyyyMMdd -> yyyy-MM-dd
                        String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
                        long userid = Long.valueOf(keySplited[1]);
                        long adid = Long.valueOf(keySplited[2]);

                        // 从mysql中查询指定日期指定用户对指定广告的点击量
                        IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                        int clickCount = adUserClickCountDAO.findClickCountByMultiKey(
                                date, userid, adid);

                        // 判断，如果点击量大于等于100，ok，那么不好意思，你就是黑名单用户
                        // 那么就拉入黑名单，返回true
                        if(clickCount >= 100) {
                            return true;
                        }

                        // 反之，如果点击量小于100的，那么就暂时不要管它了
                        return false;
                    }
                });
                /*
                * blacklistDStream
                *里面的每个batch，其实就是都是过滤出来的已经在某天对某个广告点击量超过100的用户
                *
                * 实际上，是要通过对stream执行操作，对其中的rdd中的userid进行全局的去重
                * */
                JavaDStream<Long> blacklistUseridDStream = blacklistDStream.map(new Function<Tuple2<String, Long>, Long>() {

                    private static final long serialVersionUID = -202511011640711165L;

                    @Override
                    public Long call(Tuple2<String, Long> tuple) throws Exception {
                        String key = tuple._1;
                        String[] keySplited = key.split("_");
                        Long userid = Long.valueOf(keySplited[1]);
                        return userid;
                    }
                });
                JavaDStream<Long> distinctBlacklistUseridDStream = blacklistUseridDStream.transform(new Function<JavaRDD<Long>, JavaRDD<Long>>() {
                    private static final long serialVersionUID = -1565853129109663879L;

                    @Override
                    public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
                        return rdd.distinct();
                    }
                });
                /*
                * 到这一步为止，distinctBlacklistUseridDStream
                * 每一个rdd,只包含了userid，而且还进行了全局的去重
                * 保证每一次过滤出来的黑名单用户都没有重复的
                * */
                distinctBlacklistUseridDStream.foreachRDD(new Function<JavaRDD<Long>, Void>() {
                    private static final long serialVersionUID = 4548410854637260137L;

                    @Override
                    public Void call(JavaRDD<Long> rdd) throws Exception {
                        rdd.foreachPartition(new VoidFunction<Iterator<Long>>() {
                            @Override
                            public void call(Iterator<Long> iterator) throws Exception {
                                List<AdBlacklist> adBlacklists = new ArrayList<AdBlacklist>();

                                while(iterator.hasNext()) {
                                    long userid = iterator.next();

                                    AdBlacklist adBlacklist = new AdBlacklist();
                                    adBlacklist.setUserid(userid);

                                    adBlacklists.add(adBlacklist);
                                }

                                IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
                                adBlacklistDAO.insertBatch(adBlacklists);
                            }
                        });
                        return null;
                    }
                });
                return null;
            }
        });
    }

    private static JavaPairDStream<String, String> filterByBlacklist(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        return null;
    }


}



























