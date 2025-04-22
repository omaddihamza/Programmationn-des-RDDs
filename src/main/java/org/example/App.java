package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App {
    public static void main(String[] args) {
        // Configuration Spark
        SparkConf conf = new SparkConf().setAppName("App 1 Spark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Lire les lignes du fichier ventes.txt
        JavaRDD<String> rddLines = sc.textFile("ventes.txt");

        // Ignorer l’en-tête
        String header = rddLines.first(); // Prend la première ligne
        JavaRDD<String> rddSansEntete = rddLines.filter(line -> !line.equals(header));

        JavaPairRDD<String, Double> ventesParVille = rddSansEntete.mapToPair(line -> {
            String[] fields = line.split(",");
            String ville = fields[1];
            double prix = Double.parseDouble(fields[3]);
            return new Tuple2<>(ville, prix);
        });

        JavaPairRDD<String, Double> totalParVille = ventesParVille.reduceByKey(Double::sum);

        // Affichage
        totalParVille.foreach(tuple ->
                System.out.println("Ville : " + tuple._1 + " | Total des ventes : " + tuple._2)
        );

    }
}