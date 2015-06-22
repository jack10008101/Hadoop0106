package com.web.datamining.project;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 有1，2，3，4四个网页，计算他们的pagerank的值， 得到的矩阵关系如下：
 * s={{0,0,0,0},{1/3,0,0,1},{1/3,1/2,0,0
 * },{1/3,1/2,1,0}}取alpha=0.85，通过不断的迭代，当q(next
 * )和q(current)之间的距离小于0.0000001时，认为已经收敛。pagerank就是特征值为1的特征向量
 * ，1,2,3,4号网页的价值分别为特征向量中对应维的值
 * 
 * @author jack 2015年6月22日
 *
 */
public class PageRank {
	private static final double ALPHA = 0.8;
	private static final double DISTANCE = 0.0000001;

	public static void main(String[] args) {
		System.out.println("alpha的值为：" + ALPHA);
		List<Double> q1 = new ArrayList<Double>();
		q1.add(new Double(1 / 4.0));
		q1.add(new Double(1 / 4.0));
		q1.add(new Double(1 / 4.0));
		q1.add(new Double(1 / 4.0));
		System.out.println("初始化的向量Q为：");
		printVector(q1);
		System.out.println("初始化的转移矩阵S为：");
		// printMatrix(getG(ALPHA));
		printMatrix(getS());
		//抽税法
		//List<Double> pageRank = calPageRank(q1, ALPHA);
		List<Double> pageRank=calculatePageRankNoTax(q1);
		System.out.println("PageRank为：");
		printVector(pageRank);
	}

	private static List<Double> calculatePageRankNoTax(List<Double> q1) {
		// TODO Auto-generated method stub
		List<Double> q=null;
		int count=0;
		while (true) {
			q=calculateMatrixMultiple(getS(), q1);
			double distance=calculateDistance(q, q1);
			System.out.println("向量q和向量q1之间的distance：" + distance);
			count++;
			if (distance<=DISTANCE) {
				System.out.println("向量q1：");
				printVector(q1);
				System.out.println("向量q：");
				printVector(q);
				System.out.println("迭代次数为："+count);
				break;
			}
			q1=q;
		}
		return q;
	}

	private static List<Double> calculateMatrixMultiple(List<List<Double>> s,
			List<Double> q1) {
		// TODO Auto-generated method stub
		if (s == null || q1 == null || s.size() == 0
				|| (s.get(0).size() != q1.size())) {
			return null;
		}
		List<Double> result = new ArrayList<Double>();
		for (int i = 0; i < s.size(); i++) {
			double sum = 0.0;
			for (int j = 0; j < s.get(i).size(); j++) {
				sum += s.get(i).get(j).doubleValue() * q1.get(j).doubleValue();
			}
			result.add(sum);
		}
		return result;
	}

	private static List<Double> calPageRank(List<Double> q1, double alpha2) {
		// TODO Auto-generated method stub
		/*
		 * v'=alpha*S*V+(1-alpha)U/n,其中n表示节点数V是一个n*1的列向量，S是一个n*n的矩阵 U
		 * 
		 * 是一个n*1的列向量
		 */
		List<Double> q = null;
		List<Double> secondPart = calculateVectorMultipleNum(getU(), alpha2,
				q1.size());
		int count=0;
		while (true) {
			List<Double> firstPart = calculateMatrixMultiple(getS(), q1, alpha2);
			q = addVector(firstPart, secondPart);
			double distance = calculateDistance(q, q1);
			System.out.println("向量q和向量q1之间的distance：" + distance);
			count++;
			if (distance <= DISTANCE) {
				System.out.println("向量q1：");
				printVector(q1);
				System.out.println("向量q：");
				printVector(q);
				System.out.println("迭代次数为："+count);
				break;
			}
			q1 = q;
		}
		return q;
	}

	/**
	 * 计算两个向量之间的距离
	 * 
	 * @param q
	 *            第一个向量
	 * @param q1
	 *            第二个向量
	 * @return 两个向量之间的距离
	 */
	private static double calculateDistance(List<Double> q, List<Double> q1) {
		// TODO Auto-generated method stub
		double sum = 0.0;
		if (q.size() != q1.size()) {
			return -1;
		}
		for (int i = 0; i < q1.size(); i++) {
			sum += Math
					.pow(q1.get(i).doubleValue() - q.get(i).doubleValue(), 2);
		}
		return Math.sqrt(sum);
	}

	/**
	 * 将两个N*1维的列向量相加，因为两个列向量都是N*1维，所以使用一个List<Double>来存储一个列向量
	 * 
	 * @param firstPart
	 * @param secondPart
	 * @return
	 */
	private static List<Double> addVector(List<Double> firstPart,
			List<Double> secondPart) {
		// TODO Auto-generated method stub
		if (firstPart == null || secondPart == null || firstPart.size() == 0) {
			return null;
		}
		List<Double> result = new ArrayList<Double>();
		for (int i = 0; i < firstPart.size(); i++) {
			result.add(firstPart.get(i) + secondPart.get(i));
		}
		return result;
	}

	/**
	 * 计算一个N*1的列向量乘以一个系数（1-alpha）/size
	 * 
	 * @param u
	 *            N*1的列向量
	 * @param alpha2
	 * @param size
	 *            向量的维数
	 * @return 返回一个N*1的列向量
	 */
	private static List<Double> calculateVectorMultipleNum(
			List<List<Double>> u, double alpha2, int size) {
		// TODO Auto-generated method stub
		List<Double> result = new ArrayList<Double>();
		double temp = 0.0;
		for (int i = 0; i < u.size(); i++) {
			for (int j = 0; j < 1; j++) {
				temp = u.get(i).get(j).doubleValue() * (1 - alpha2) / size;
			}
			result.add(temp);
		}

		return result;
	}

	/**
	 * 计算一个矩阵乘以一个向量，最后的结果乘以一个alpha，返回一个向量
	 * 
	 * @param s
	 *            一个矩阵
	 * @param q1
	 *            一个向量
	 * @param alpha2
	 *            一个系数
	 * @return 返回一个向量N*1
	 */
	private static List<Double> calculateMatrixMultiple(List<List<Double>> s,
			List<Double> q1, double alpha2) {
		// TODO Auto-generated method stub
		if (s == null || q1 == null || s.size() == 0
				|| (s.get(0).size() != q1.size())) {
			return null;
		}
		List<Double> result = new ArrayList<Double>();
		for (int i = 0; i < s.size(); i++) {
			double sum = 0.0;
			for (int j = 0; j < s.get(i).size(); j++) {
				sum += s.get(i).get(j).doubleValue() * q1.get(j).doubleValue();
			}
			result.add(sum * alpha2);
		}
		return result;
	}

	/**
	 * 打印输出一个矩阵
	 * 
	 * @param g
	 */
	private static void printMatrix(List<List<Double>> g) {
		// TODO Auto-generated method stub
		for (int i = 0; i < g.size(); i++) {
			for (int j = 0; j < g.get(i).size(); j++) {
				System.out.print(g.get(i).get(j) + ",");
			}
			System.out.println();
		}
	}

	/**
	 * 计算获得初始的G矩阵
	 * 
	 * @param alpha2
	 *            =0.85
	 * @return 初始矩阵
	 */
	private static List<List<Double>> getG(double alpha2) {
		// TODO Auto-generated method stub
		int num = getS().size();
		List<List<Double>> aS = numberMulMatrix(getS(), alpha2);
		return null;
	}

	private static List<List<Double>> numberMulMatrix(List<List<Double>> s,
			double alpha2) {
		// TODO Auto-generated method stub

		return null;
	}

	/**
	 * 返回初始化转移矩阵S
	 * 
	 * @return
	 */
	private static List<List<Double>> getS() {
		// TODO Auto-generated method stub
		List<Double> row1 = new ArrayList<Double>();
/*		row1.add(new Double(0));
		row1.add(new Double(1 / 2.0));
		row1.add(new Double(0));
		row1.add(new Double(0));*/
		row1.add(new Double(0));
		row1.add(new Double(1 / 2.0));
		row1.add(new Double(1));
		row1.add(new Double(0));
		List<Double> row2 = new ArrayList<Double>();
/*		row2.add(new Double(1 / 3.0));
		row2.add(new Double(0));
		row2.add(new Double(0));
		row2.add(new Double(1 / 2.0));*/
		row2.add(new Double(1 / 3.0));
		row2.add(new Double(0));
		row2.add(new Double(0));
		row2.add(new Double(1 / 2.0));
		List<Double> row3 = new ArrayList<Double>();
/*		row3.add(new Double(1 / 3.0));
		row3.add(new Double(0));
		row3.add(new Double(1));
		row3.add(new Double(1 / 2.0));*/
		row3.add(new Double(1 / 3.0));
		row3.add(new Double(0));
		row3.add(new Double(0));
		row3.add(new Double(1 / 2.0));
		List<Double> row4 = new ArrayList<Double>();
		row4.add(new Double(1 / 3.0));
		row4.add(new Double(1 / 2.0));
		row4.add(new Double(0));
		row4.add(new Double(0));
		List<List<Double>> result = new ArrayList<List<Double>>();
		result.add(row1);
		result.add(row2);
		result.add(row3);
		result.add(row4);
		return result;
	}

	/**
	 * 初始化矩阵U，全为1，v'=alpha*S*V+(1-alpha)U/n,其中n表示节点数V是一个n*1的列向量，S是一个
	 * 
	 * n*n的矩阵 U是一个n*1的列向量
	 * 
	 * @return
	 */
	public static List<List<Double>> getU() {
		List<Double> row1 = new ArrayList<Double>();
		row1.add(new Double(1));
		List<Double> row2 = new ArrayList<Double>();
		row2.add(new Double(1));
		List<Double> row3 = new ArrayList<Double>();
		row3.add(new Double(1));
		List<Double> row4 = new ArrayList<Double>();
		row4.add(new Double(1));
		List<List<Double>> result = new ArrayList<List<Double>>();
		result.add(row1);
		result.add(row2);
		result.add(row3);
		result.add(row4);
		return result;
	}

	/**
	 * 打印输出一个向量
	 * 
	 * @param q1
	 */
	private static void printVector(List<Double> q1) {
		// TODO Auto-generated method stub
		for (int i = 0; i < q1.size(); i++) {
			System.out.print(q1.get(i) + ",");
		}
		System.out.println();
	}

	/**
	 * 获得一个随机的初始向量Q
	 * 
	 * @param n
	 *            向量的维数
	 * @return 一个随机向量，每一维的数字大小在0-5之间的随机数
	 */
	private static List<Double> getInitQ(int n) {
		Random random = new Random();
		List<Double> qDoubles = new ArrayList<Double>();
		for (int i = 0; i < n; i++) {
			qDoubles.add(new Double(random.nextDouble() * 5));
		}
		return qDoubles;

	}
}
