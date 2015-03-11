package com.hadoop.trial.lucene;

import java.io.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

public class IKAnalyzerTest
{
	public static String str = "IK Analyzer是一个开源的，基于java语言" + "开发的轻量级的中文分词工具包。从2006年12月推出1.0版开始， "
			+ "IKAnalyzer已经推出了4个大版本。最初，它是以开源项目Luence为" + "应用主体的，结合词典分词和文法分析算法的中文分词组件。从3.0版"
			+ "本开始，IK发展为面向Java的公用分词组件，独立于Lucene项目，同时" + "提供了对Lucene的默认优化实现。";

	public static void main(String args[]) throws IOException
	{

		// 基于Lucene实现
		Analyzer analyzer = new IKAnalyzer(true);// true智能切分
		StringReader reader = new StringReader(str);
		TokenStream ts = analyzer.tokenStream("", reader);
		CharTermAttribute term = ts.getAttribute(CharTermAttribute.class);
		while (ts.incrementToken())
		{
			System.out.print(term.toString() + "|");
		}
		reader.close();
		System.out.println();
		analyzer.close();

		// // 独立Lucene实现
		// StringReader re = new StringReader(str);
		// IKSegmenter ik = new IKSegmenter(re, true);
		// Lexeme lex = null;
		// while ((lex = ik.next()) != null)
		// {
		// System.out.print(lex.getLexemeText() + "|");
		// }
	}
}