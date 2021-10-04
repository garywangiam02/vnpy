#include "Main.h"
#include <iostream>
#include <fstream>

using namespace std;

#if defined(_MSC_VER)
//  Microsoft 
#define EXPORT __declspec(dllexport)
#define IMPORT __declspec(dllimport)
#elif defined(__GNUC__)
//  GCC
#define EXPORT __attribute__((visibility("default")))
#define IMPORT
#else
//  do nothing and hope for the best?
#define EXPORT
#define IMPORT
#pragma warning Unknown dynamic link import/export semantics.
#endif

extern "C" {

//=============================================================================
// 输出函数1号：K线方向
//=============================================================================

void Func1(int nCount, float *pOut, float *pIn, float *pHigh, float *pLow)
{
    float *pDirection = new float[nCount];
    float *pOutHigh = new float[nCount];
    float *pOutLow = new float[nCount];
    float *pInclude = new float[nCount];

    BaoHan(nCount, pDirection, pOutHigh, pOutLow, pInclude, pHigh, pLow);

    for (int i = 0; i < nCount; i++)
    {
        pOut[i] = pDirection[i];
    }

    delete []pDirection;
    delete []pOutHigh;
    delete []pOutLow;
    delete []pInclude;
}

//=============================================================================
// 输出函数2号：是否是有包含关系的K线
//=============================================================================
void Func2(int nCount, float *pOut, float *pIn, float *pHigh, float *pLow)
{
    float *pDirection = new float[nCount];
    float *pOutHigh = new float[nCount];
    float *pOutLow = new float[nCount];
    float *pInclude = new float[nCount];

    BaoHan(nCount, pDirection, pOutHigh, pOutLow, pInclude, pHigh, pLow);

    for (int i = 0; i < nCount; i++)
    {
        pOut[i] = pInclude[i];
    }

    delete []pDirection;
    delete []pOutHigh;
    delete []pOutLow;
    delete []pInclude;
}

//=============================================================================
// 输出函数3号：包含处理后的K线高
//=============================================================================
void Func3(int nCount, float *pOut, float *pIn, float *pHigh, float *pLow)
{
    float *pDirection = new float[nCount];
    float *pOutHigh = new float[nCount];
    float *pOutLow = new float[nCount];
    float *pInclude = new float[nCount];

    BaoHan(nCount, pDirection, pOutHigh, pOutLow, pInclude, pHigh, pLow);

    for (int i = 0; i < nCount; i++)
    {
        pOut[i] = pOutHigh[i];
    }

    delete []pDirection;
    delete []pOutHigh;
    delete []pOutLow;
    delete []pInclude;
}

//=============================================================================
// 输出函数4号：包含处理后的K线低
//=============================================================================
void Func4(int nCount, float *pOut, float *pIn, float *pHigh, float *pLow)
{
    float *pDirection = new float[nCount];
    float *pOutHigh = new float[nCount];
    float *pOutLow = new float[nCount];
    float *pInclude = new float[nCount];

    BaoHan(nCount, pDirection, pOutHigh, pOutLow, pInclude, pHigh, pLow);

    for (int i = 0; i < nCount; i++)
    {
        pOut[i] = pOutLow[i];
    }

    delete []pDirection;
    delete []pOutHigh;
    delete []pOutLow;
    delete []pInclude;
}

//=============================================================================
// 输出函数5号：处理一下包含信号，方便通达信画线
//=============================================================================
void Func5(int nCount, float *pOut, float *pIn1, float *pIn2, float *pInclude)
{
    pOut[0] = 0;
    float n = 1;
    for (int i = 1; i < nCount; i++)
    {
        if (pInclude[i-1] == 0 && pInclude[i] == 0)
        {
            pOut[i] = 0;
        }
        else if (pInclude[i-1] == 0 && pInclude[i] == 1)
        {
            pOut[i-1] = n;
            pOut[i] = n;
        }
        else if (pInclude[i-1] == 1 && pInclude[i] == 1)
        {
            pOut[i] = n;
        }
        else if (pInclude[i-1] == 1 && pInclude[i] == 0)
        {
            pOut[i] = 0;
            if (n == 1)
            {
                n = 2;
            }
            else
            {
                n = 1;
            }

        }
    }
}

//=============================================================================
// 输出函数6号：输出笔顶底端点
//=============================================================================
void Func6(int nCount, float *pOut, float *pIn, float *pHigh, float *pLow, int iBi = 0, int iFenXingQuJian = 2)
{
    if (iBi == 0)
    {
        Bi0(nCount, pOut, pIn, pHigh, pLow);
    }
    else if (iBi == 1)
    {
        Bi1(nCount, pOut, pIn, pHigh, pLow, iFenXingQuJian);
    }
    else if (iBi == 2)
    {
        Bi2(nCount, pOut, pIn, pHigh, pLow, iFenXingQuJian);
    }
    else
    {
        Bi0(nCount, pOut, pIn, pHigh, pLow);
    }
}

//=============================================================================
// 输出函数7号：线段顶底信号
//=============================================================================
void Func7(int nCount, float *pOut, float *pIn, float *pHigh, float *pLow, int iDuan = 0)
{
    if (iDuan == 0)
    {
        Duan0(nCount, pOut, pIn, pHigh, pLow);
    }
    else if (iDuan == 1)
    {
        Duan1(nCount, pOut, pIn, pHigh, pLow);
    }
    else
    {
        Duan0(nCount, pOut, pIn, pHigh, pLow);
    }
}

//=============================================================================
// 输出函数8号：寻找中枢高点数据
// pOut 中枢高位
// pIn 段
// pHigh K线高点列表
// pLow K线低点列表
//=============================================================================
void Func8(int nCount, float *pOut, float *pIn, float *pHigh, float *pLow)
{
    ZhongShu ZhongShuOne;

	//printf("count:%d\n",nCount);

    for (int i = 0; i < nCount; i++)
    {	
        if (pIn[i] == 1)
        {
			//printf("%d , high:%f \n", i,pHigh[i]);
            // 遇到线段高点，推入中枢算法
            if (ZhongShuOne.PushHigh(i, pHigh[i]))
            {
                bool bValid = true;
                float fHighValue;  // 记录高点的数值
                int nHighIndex;    // 中枢高点所在索引位置
                int nLowIndex;     // 中枢低点所在索引位置
                int nLowIndexTemp; // 低点临时
                int nHighCount = 0; // 记录一共有多少个高点
                if (ZhongShuOne.nDirection == 1 && ZhongShuOne.nTerminate == -1) // 向上中枢被向下终结
                {
                    bValid = false;
					// 从中枢的开始=》结束，逐一扫描
                    for (int x = ZhongShuOne.nStart; x <= ZhongShuOne.nEnd; x++)
                    {
						// 属于段、或分笔的高点
                        if (pIn[x] == 1)
                        {
							// 出现首个高点
                            if (nHighCount == 0)
                            {
                                nHighCount++;  
                                fHighValue = pHigh[x];  // 中枢高点=当前高点
                                nHighIndex = x;         // 记录中枢高点的所在索引
								//printf("Func8 nHighCount:%d, fHighValue:%f, nHighIndex:%d \n", nHighCount, fHighValue, nHighIndex);
                            }
                            else
                            {
                                nHighCount++;
								// 如果新的高点比中枢高点高，更新
                                if (pHigh[x] >= fHighValue)
                                {
									// 出现两个高点时，中枢成立
                                    if (nHighCount > 2)
                                    {
                                        bValid = true;
                                    }
									float pre_high_value = fHighValue;
                                    fHighValue = pHigh[x];
                                    nHighIndex = x;
									// 记录中枢低点所在位置
                                    nLowIndex = nLowIndexTemp;
									//printf("Func8 nHighCount:%d, fHighValue:%f => %f, nHighIndex:%d nLowIndex:%d\n", nHighCount, pre_high_value, fHighValue, nHighIndex, nLowIndex);
                                }
                            }
                        }
                        else if (pIn[x] == -1)
                        {
							// 临时记录中枢低点所在位置
                            nLowIndexTemp = x;
                        }
                    }
					// 有效中枢
                    if (bValid)
                    {
                        ZhongShuOne.nEnd = nLowIndex; // 中枢结束点移到最高点的前一个低点。
						//printf("Func8 Valid Zhongshu.start:%d => end:%d", ZhongShuOne.nStart, ZhongShuOne.nEnd);
                    }
					//else {
						//cout << "Func8 not valid zhongshu\n" << endl;
					//}

					// 下一个中枢的寻找位置
					int pre_i = i;					
                    i = nHighIndex - 1;
					//printf("Func8 zs high terminal, %d => %d\n", pre_i, i);
                }
                else
                {
					int pre_i = i;
                    i = ZhongShuOne.nEnd - 1;
					//printf("Func8 high i %d => %d\n", pre_i, i);
                }
                if (bValid)
                {
                    // 区段内更新算得的中枢高数据
                    for (int j = ZhongShuOne.nStart + 1; j <= ZhongShuOne.nEnd - 1; j++)
                    {
                        pOut[j] = ZhongShuOne.fHigh;
                    }
					//printf("Func8 zhongshuOne found:%d=>%d, low:%f,high:%f", ZhongShuOne.nStart, ZhongShuOne.nEnd, ZhongShuOne.fLow, ZhongShuOne.fHigh);
                }
				
                ZhongShuOne.Reset();
            }
        }
        else if (pIn[i] == -1)
        {
			//printf("%d , low:%f\n", i, pHigh[i]);
            // 遇到线段低点，推入中枢算法
            if (ZhongShuOne.PushLow(i, pLow[i]))
            {
                bool bValid = true;
                float fLowValue;
                int nLowIndex;    // 中枢最低点的索引
                int nHighIndex;   // 中枢最高点的索引
                int nHighIndexTemp; //临时记录高点位置索引
                int nLowCount = 0;  // 下跌线段计数
                if (ZhongShuOne.nDirection == -1 && ZhongShuOne.nTerminate == 1) // 向下中枢被向上终结
                {
                    bValid = false;
					// 从中枢开始=》结束，逐一检查
                    for (int x = ZhongShuOne.nStart; x <= ZhongShuOne.nEnd; x++)
                    {
						// 当前是下跌线段
                        if (pIn[x] == -1)
                        {
							// 下跌计数器+1
                            if (nLowCount == 0)
                            {
                                nLowCount++;
                                fLowValue = pLow[x];    // 中枢最低值
                                nLowIndex = x;
								//printf("Func8 nLowCount:%d, fLowValue:%f, nLowIndex:%d \n", nLowCount, fLowValue, nLowIndex);
                            }
                            else
                            {
                                nLowCount++;
                                if (pLow[x] <= fLowValue)
                                {	
									// 出现两个低点时，中枢成立
                                    if (nLowCount > 2)
                                    {
                                        bValid = true;
                                    }
									float pre_low_value = fLowValue;
                                    fLowValue = pLow[x];
                                    nLowIndex = x;
                                    nHighIndex = nHighIndexTemp;
									//printf("Func8 nLowCount:%d, fLowValue:%f => %f, nLowIndex:%d nHighIndex:%d \n", nLowCount, pre_low_value, fLowValue, nLowIndex, nHighIndex);
                                }
                            }
                        }
                        else if (pIn[x] == 1)
                        {
                            nHighIndexTemp = x;
                        }
                    }
                    if (bValid)
                    {
                        ZhongShuOne.nEnd = nHighIndex; // 中枢结束点移到最高点的前一个低点。
						//printf("Func8 Valid Zhongshu.start:%d => end:%d", ZhongShuOne.nStart, ZhongShuOne.nEnd);
                    }
					else {
						//cout<<"not valid zhongshu"<<endl;
					}
					
					//int pre_i = i;
					i = nLowIndex - 1;
					//printf("Func8 zs low terminal at: %d\n", i);
                    
                }
                else
                {
					//int pre_i = i;
                    i = ZhongShuOne.nEnd - 1;
					//printf("Func 8 low i %d => %d\n", pre_i, i);
                }
                if (bValid)
                {
                    // 区段内更新算得的中枢高数据
                    for (int j = ZhongShuOne.nStart + 1; j <= ZhongShuOne.nEnd - 1; j++)
                    {
                        pOut[j] = ZhongShuOne.fHigh;
                    }
					//printf("Func8 zhongshuOne found:%d=>%d, low:%f,high:%f", ZhongShuOne.nStart, ZhongShuOne.nEnd, ZhongShuOne.fLow, ZhongShuOne.fHigh);
                }
				
                ZhongShuOne.Reset();
            }
        }
    }
	//printf("Func8 end loop\n");
    if (ZhongShuOne.bValid) // 最后一个还没有被终结的中枢。
    {
		//printf("no ended zhongshu ");
        // 区段内更新算得的中枢高数据
        for (int j = ZhongShuOne.nStart + 1; j <= ZhongShuOne.nEnd - 1; j++)
        {
            pOut[j] = ZhongShuOne.fHigh;
        }
    }
}

//=============================================================================
// 输出函数9号：寻找中枢低点数据
//=============================================================================
void Func9(int nCount, float *pOut, float *pIn, float *pHigh, float *pLow)
{

    ZhongShu ZhongShuOne;

    for (int i = 0; i < nCount; i++)
    {
        if (pIn[i] == 1)
        {
            // 遇到线段高点，推入中枢算法
            if (ZhongShuOne.PushHigh(i, pHigh[i]))
            {
                bool bValid = true;
                float fHighValue;
                int nHighIndex;
                int nLowIndex;
                int nLowIndexTemp;
                int nHighCount = 0;
                if (ZhongShuOne.nDirection == 1 && ZhongShuOne.nTerminate == -1) // 向上中枢被向下终结
                {
                    bValid = false;
                    for (int x = ZhongShuOne.nStart; x <= ZhongShuOne.nEnd; x++)
                    {
                        if (pIn[x] == 1)
                        {
                            if (nHighCount == 0)
                            {
                                nHighCount++;
                                fHighValue = pHigh[x];
                                nHighIndex = x;
								//printf("Func9 nHighCount:%d, fHighValue:%f, nHighIndex:%d \n", nHighCount, fHighValue, nHighIndex);
                            }
                            else
                            {
                                nHighCount++;
                                if (pHigh[x] >= fHighValue)
                                {
                                    if (nHighCount > 2)
                                    {
                                        bValid = true;
                                    }
									float pre_high_value = fHighValue;
                                    fHighValue = pHigh[x];
                                    nHighIndex = x;
                                    nLowIndex = nLowIndexTemp;
									//printf("Func9 nHighCount:%d, fHighValue:%f => %f, nHighIndex:%d nLowIndex:%d\n", nHighCount, pre_high_value, fHighValue, nHighIndex, nLowIndex);
                                }
                            }
                        }
                        else if (pIn[x] == -1)
                        {
                            nLowIndexTemp = x;
                        }
                    }
                    if (bValid)
                    {
                        ZhongShuOne.nEnd = nLowIndex; // 中枢结束点移到最高点的前一个低点。
						printf("Func9 Valid Zhongshu.start:%d => end:%d", ZhongShuOne.nStart, ZhongShuOne.nEnd);
                    }

					//int pre_i = i;
					i = nHighIndex - 1;
					//printf("Func9 zs high terminal, %d => %d\n", pre_i, i);
                }
                else
                {
					//int pre_i = i;
					i = ZhongShuOne.nEnd - 1;
					//printf("Func9 high i %d => %d\n", pre_i, i);
                    
                }
                if (bValid)
                {
                    // 区段内更新算得的中枢低数据
                    for (int j = ZhongShuOne.nStart + 1; j <= ZhongShuOne.nEnd - 1; j++)
                    {
                        pOut[j] = ZhongShuOne.fLow;
                    }

                }

                ZhongShuOne.Reset();
            }
        }
        else if (pIn[i] == -1)
        {
            // 遇到线段低点，推入中枢算法
            if (ZhongShuOne.PushLow(i, pLow[i]))
            {
                bool bValid = true;
                float fLowValue;
                int nLowIndex;
                int nHighIndex;
                int nHighIndexTemp;
                int nLowCount = 0;
                if (ZhongShuOne.nDirection == -1 && ZhongShuOne.nTerminate == 1) // 向下中枢被向上终结
                {
                    bValid = false;
                    for (int x = ZhongShuOne.nStart; x <= ZhongShuOne.nEnd; x++)
                    {
                        if (pIn[x] == -1)
                        {
                            if (nLowCount == 0)
                            {
                                nLowCount++;
                                fLowValue = pLow[x];
                                nLowIndex = x;
								//printf("Func9 nLowCount:%d, fLowValue:%f, nLowIndex:%d \n", nLowCount, fLowValue, nLowIndex);
                            }
                            else
                            {
                                nLowCount++;
                                if (pLow[x] <= fLowValue)
                                {
                                    if (nLowCount > 2)
                                    {
                                        bValid = true;
                                    }
									float pre_low_value = fLowValue;
                                    fLowValue = pLow[x];
                                    nLowIndex = x;
                                    nHighIndex = nHighIndexTemp;
									//printf("Func9 nLowCount:%d, fLowValue:%f => %f, nLowIndex:%d nHighIndex:%d \n", nLowCount, pre_low_value, fLowValue, nLowIndex, nHighIndex);
                                }
                            }
                        }
                        else if (pIn[x] == 1)
                        {
                            nHighIndexTemp = x;
                        }
                    }
                    if (bValid)
                    {
                        ZhongShuOne.nEnd = nHighIndex; // 中枢结束点移到最高点的前一个低点。
						//printf("Func9 Valid Zhongshu.start:%d => end:%d", ZhongShuOne.nStart, ZhongShuOne.nEnd);
                    }                    
					//int pre_i = i;
				    i = nLowIndex - 1;
					//printf("zs low terminal, %d => %d\n", pre_i, i);

                }
                else
                {
					int pre_i = i;
                    i = ZhongShuOne.nEnd - 1;
					//printf("low i %d => %d\n", pre_i, i);
                }
                if (bValid)
                {
                    // 区段内更新算得的中枢低数据
                    for (int j = ZhongShuOne.nStart + 1; j <= ZhongShuOne.nEnd - 1; j++)
                    {
                        pOut[j] = ZhongShuOne.fLow;
                    }

                }

                ZhongShuOne.Reset();
            }
        }
    }
    if (ZhongShuOne.bValid)
    {
        // 区段内更新算得的中枢低数据
        for (int j = ZhongShuOne.nStart + 1; j <= ZhongShuOne.nEnd - 1; j++)
        {
            pOut[j] = ZhongShuOne.fLow;
        }
    }
}

//=============================================================================
// 输出函数10号：中枢起点、终点信号
//=============================================================================
void Func10(int nCount, float *pOut, float *pIn, float *pHigh, float *pLow)
{

    //std::ofstream fout;
    //fout.open("D:\\CHANLUN.TXT", std::ofstream::out);

    ZhongShu ZhongShuOne;

    for (int i = 0; i < nCount; i++)
    {
        if (pIn[i] == 1)
        {
            // 遇到线段高点，推入中枢算法
            if (ZhongShuOne.PushHigh(i, pHigh[i]))
            {
                //fout<<"中枢终结"<<pHigh[i]<<endl;
                bool bValid = true;
                float fHighValue;
                int nHighIndex;
                int nLowIndex;
                int nLowIndexTemp;
                int nHighCount = 0;
                if (ZhongShuOne.nDirection == 1 && ZhongShuOne.nTerminate == -1) // 向上中枢被向下终结
                {
                    //fout<<"向上中枢被向下终结"<<endl;
                    bValid = false;
                    for (int x = ZhongShuOne.nStart; x <= ZhongShuOne.nEnd; x++)
                    {
                        if (pIn[x] == 1)
                        {
                            if (nHighCount == 0)
                            {
                                nHighCount++;
                                fHighValue = pHigh[x];
                                nHighIndex = x;
                            }
                            else
                            {
                                nHighCount++;
                                if (pHigh[x] >= fHighValue)
                                {
                                    if (nHighCount > 2)
                                    {
                                        bValid = true;
                                    }
                                    fHighValue = pHigh[x];
                                    nHighIndex = x;
                                    nLowIndex = nLowIndexTemp;
                                }
                            }
                        }
                        else if (pIn[x] == -1)
                        {
                            nLowIndexTemp = x;
                        }
                    }
                    if (bValid)
                    {
                        //fout<<"同级别分解保留最后中枢"<<endl;
                        //fout<<"中枢结束点移到"<<pLow[nLowIndex]<<endl;
                        ZhongShuOne.nEnd = nLowIndex; // 中枢结束点移到最高点的前一个低点。
                    }
                    else
                    {
                        //fout<<"同级别分解最后中枢无效"<<endl;
                    }
                    i = nHighIndex - 1;
                }
                else
                {
                    //fout<<"向下中枢被向下终结"<<endl;
                    i = ZhongShuOne.nEnd - 1;
                }
                if (bValid)
                {
                    // 进行标记
                    pOut[ZhongShuOne.nStart + 1] = 1;
                    pOut[ZhongShuOne.nEnd - 1]   = 2;
                }
                ZhongShuOne.Reset();
            }
        }
        else if (pIn[i] == -1)
        {
            // 遇到线段低点，推入中枢算法
            if (ZhongShuOne.PushLow(i, pLow[i]))
            {
                //fout<<"中枢终结"<<pHigh[i]<<endl;
                bool bValid = true;
                float fLowValue;
                int nLowIndex;
                int nHighIndex;
                int nHighIndexTemp;
                int nLowCount = 0;
                if (ZhongShuOne.nDirection == -1 && ZhongShuOne.nTerminate == 1) // 向下中枢被向上终结
                {
                    //fout<<"向下中枢被向上终结"<<endl;
                    bValid = false;
                    for (int x = ZhongShuOne.nStart; x <= ZhongShuOne.nEnd; x++)
                    {
                        if (pIn[x] == -1)
                        {
                            if (nLowCount == 0)
                            {
                                nLowCount++;
                                fLowValue = pLow[x];
                                nLowIndex = x;
                            }
                            else
                            {
                                nLowCount++;
                                if (pLow[x] <= fLowValue)
                                {
                                    if (nLowCount > 2)
                                    {
                                        bValid = true;
                                    }
                                    fLowValue = pLow[x];
                                    nLowIndex = x;
                                    nHighIndex = nHighIndexTemp;
                                }
                            }
                            //fout<<"低点数量"<<nLowCount<<endl;
                        }
                        else if (pIn[x] == 1)
                        {
                            nHighIndexTemp = x;
                        }
                    }
                    if (bValid)
                    {
                        //fout<<"同级别分解保留最后中枢"<<endl;
                        //fout<<"中枢结束点移到"<<pHigh[nHighIndex]<<endl;
                        ZhongShuOne.nEnd = nHighIndex; // 中枢结束点移到最高点的前一个低点。
                    }
                    else
                    {
                        //fout<<"同级别分解最后中枢无效"<<endl;
                    }
                    i = nLowIndex - 1;
                }
                else
                {
                    //fout<<"向上中枢被向上终结"<<endl;
                    i = ZhongShuOne.nEnd - 1;
                }
                if (bValid)
                {
                    // 进行标记
                    pOut[ZhongShuOne.nStart + 1] = 1;
                    pOut[ZhongShuOne.nEnd - 1]   = 2;

                }
                ZhongShuOne.Reset();
            }
        }
    }
    if (ZhongShuOne.bValid)
    {
        // 进行标记
        pOut[ZhongShuOne.nStart + 1] = 1;
        pOut[ZhongShuOne.nEnd - 1]   = 2;
    }
    //fout.close();
}

//=============================================================================
// 输出函数11号：中枢方向数据
//=============================================================================
void Func11(int nCount, float *pOut, float *pIn, float *pHigh, float *pLow)
{

    ZhongShu ZhongShuOne;

    for (int i = 0; i < nCount; i++)
    {
        if (pIn[i] == 1)
        {
            // 遇到线段高点，推入中枢算法
            if (ZhongShuOne.PushHigh(i, pHigh[i]))
            {
                bool bValid = true;
                float fHighValue;
                int nHighIndex;
                int nLowIndex;
                int nLowIndexTemp;
                int nHighCount = 0;
                if (ZhongShuOne.nDirection == 1 && ZhongShuOne.nTerminate == -1) // 向上中枢被向下终结
                {
                    bValid = false;
                    for (int x = ZhongShuOne.nStart; x <= ZhongShuOne.nEnd; x++)
                    {
                        if (pIn[x] == 1)
                        {
                            if (nHighCount == 0)
                            {
                                nHighCount++;
                                fHighValue = pHigh[x];
                                nHighIndex = x;
                            }
                            else
                            {
                                nHighCount++;
                                if (pHigh[x] >= fHighValue)
                                {
                                    if (nHighCount > 2)
                                    {
                                        bValid = true;
                                    }
                                    fHighValue = pHigh[x];
                                    nHighIndex = x;
                                    nLowIndex = nLowIndexTemp;
                                }
                            }
                        }
                        else if (pIn[x] == -1)
                        {
                            nLowIndexTemp = x;
                        }
                    }
                    if (bValid)
                    {
                        ZhongShuOne.nEnd = nLowIndex; // 中枢结束点移到最高点的前一个低点。
                    }
                    i = nHighIndex - 1;
                }
                else
                {
                    i = ZhongShuOne.nEnd - 1;
                }
                if (bValid)
                {
                    // 区段内更新算得的中枢方向数据
                    for (int j = ZhongShuOne.nStart + 1; j <= ZhongShuOne.nEnd - 1; j++)
                    {
                        pOut[j] = (float) ZhongShuOne.nDirection;
                    }

                }

                ZhongShuOne.Reset();
            }
        }
        else if (pIn[i] == -1)
        {
            // 遇到线段低点，推入中枢算法
            if (ZhongShuOne.PushLow(i, pLow[i]))
            {
                bool bValid = true;
                float fLowValue;
                int nLowIndex;
                int nHighIndex;
                int nHighIndexTemp;
                int nLowCount = 0;
                if (ZhongShuOne.nDirection == -1 && ZhongShuOne.nTerminate == 1) // 向下中枢被向上终结
                {
                    bValid = false;
                    for (int x = ZhongShuOne.nStart; x <= ZhongShuOne.nEnd; x++)
                    {
                        if (pIn[x] == -1)
                        {
                            if (nLowCount == 0)
                            {
                                nLowCount++;
                                fLowValue = pLow[x];
                                nLowIndex = x;
                            }
                            else
                            {
                                nLowCount++;
                                if (pLow[x] <= fLowValue)
                                {
                                    if (nLowCount > 2)
                                    {
                                        bValid = true;
                                    }
                                    fLowValue = pLow[x];
                                    nLowIndex = x;
                                    nHighIndex = nHighIndexTemp;
                                }
                            }
                        }
                        else if (pIn[x] == 1)
                        {
                            nHighIndexTemp = x;
                        }
                    }
                    if (bValid)
                    {
                        ZhongShuOne.nEnd = nHighIndex; // 中枢结束点移到最高点的前一个低点。
                    }
                    i = nLowIndex - 1;
                }
                else
                {
                    i = ZhongShuOne.nEnd - 1;
                }
                if (bValid)
                {
                    // 区段内更新算得的中枢方向数据
                    for (int j = ZhongShuOne.nStart + 1; j <= ZhongShuOne.nEnd - 1; j++)
                    {
                        pOut[j] = (float) ZhongShuOne.nDirection;
                    }

                }

                ZhongShuOne.Reset();
            }
        }
    }
    if (ZhongShuOne.bValid)
    {
        // 区段内更新算得的中枢方向数据
        for (int j = ZhongShuOne.nStart + 1; j <= ZhongShuOne.nEnd - 1; j++)
        {
            pOut[j] = (float) ZhongShuOne.nDirection;
        }
    }
}


//=============================================================================
// 输出中枢：中枢高、低，开始、结束、方向数据
//=============================================================================
void FindZhongshu(int nCount, float *pHighOut, float *pLowOut, float *pRangeOut, float *pDirectionOut, float *pIn, float *pHigh, float *pLow)
{

	ZhongShu ZhongShuOne;
	int loop_count = 0;
	int loop_max = 100;
	int minNextHighIndex = 0;
	int minNextLowIndex = 0;
	//printf("start FindZhongshu\n");
	for (int i = 0; i < nCount; i++)
	{
		
		if (pIn[i] == 1)
		{
			// 遇到线段高点，推入中枢算法
			if (ZhongShuOne.PushHigh(i, pHigh[i]))
			{
				bool bValid = true;
				float fHighValue;
				int nHighIndex;
				int nNextHighIndex; // 中枢高点后的下一个高点索引
				int nHighIndexCount = 0; // 中枢高点出现在所有高点中的第几个
				int nLowIndex;
				int nLowIndexTemp;
				int nHighCount = 0;
				if (ZhongShuOne.nDirection == 1 && ZhongShuOne.nTerminate == -1) // 向上中枢被向下终结
				{
					bValid = false;
					loop_count = 0;
					for (int x = ZhongShuOne.nStart; x <= ZhongShuOne.nEnd; x++)
					{
						//loop_count = loop_count + 1;
						//if (loop_count > loop_max)
						//{
						//	printf("FindZhongshu loop force break");
						//	break;
						//}
						if (pIn[x] == 1)
						{
							if (nHighCount == 0)
							{
								nHighCount++;
								fHighValue = pHigh[x];
								nHighIndex = x;
								nNextHighIndex = x;
								nHighIndexCount++;
								// printf("nHighIndex:%d, nHighCount:%d\n", nHighIndex, nHighCount);
							}
							else
							{
								nHighCount++;
								if (pHigh[x] >= fHighValue)
								{
									if (nHighCount > 2)
									{
										bValid = true;
									}
									fHighValue = pHigh[x];
									nHighIndex = x;
									nLowIndex = nLowIndexTemp;
									nHighIndexCount++;

								}
								else
								{
									if (nNextHighIndex == nHighIndex)
									{
										nNextHighIndex = x;
									}
								}
							
							//printf("nHighIndex:%d, nHighCount:%d\n", nHighIndex, nHighCount);
							}
						}
						else if (pIn[x] == -1)
						{
						nLowIndexTemp = x;
						}
					}
					if (bValid)
					{
						ZhongShuOne.nEnd = nLowIndex; // 中枢结束点移到最高点的前一个低点。
					}
					
					if (nHighIndexCount > 1) // 中枢高点，不是在第一个
					{	
						i = nHighIndex - 1;
					}
					else  // 中枢高点，在第一个
					{	if (nNextHighIndex > minNextHighIndex -1)
						{
							i = nNextHighIndex - 1;
							minNextHighIndex = i;
						}
						else
						{							
							minNextHighIndex = minNextHighIndex + 1;
							i = minNextHighIndex + 1;
							printf("minNextHighIndex => %d\n", minNextHighIndex);
						}										
					}
				}
				else
				{
				i = ZhongShuOne.nEnd - 1;
				}
				if (bValid)
				{
					// 进行中枢开始、结束的标记
					pRangeOut[ZhongShuOne.nStart + 1] = 1;
					pRangeOut[ZhongShuOne.nEnd - 1] = 2;

					// 区段内更新算得的中枢高、低、方向数据
					for (int j = ZhongShuOne.nStart + 1; j <= ZhongShuOne.nEnd - 1; j++)
					{
						pHighOut[j] = ZhongShuOne.fHigh;
						pLowOut[j] = ZhongShuOne.fLow;
						pDirectionOut[j] = (float) ZhongShuOne.nDirection;
					}

				}

				ZhongShuOne.Reset();
			}
		}
		else if (pIn[i] == -1)
		{
		// 遇到线段低点，推入中枢算法
		if (ZhongShuOne.PushLow(i, pLow[i]))
		{
			bool bValid = true;
			float fLowValue;
			int nLowIndex;    // 中枢低点的索引
			int nNextLowIndex; // 中枢低点后的低点索引
			int nLowIndexCount = 0; // 中枢低点出现在所有低点中的第几个
			int nHighIndex;       //中枢高点
			int nHighIndexTemp;   
			int nLowCount = 0;
			if (ZhongShuOne.nDirection == -1 && ZhongShuOne.nTerminate == 1) // 向下中枢被向上终结
			{
				bValid = false;
				loop_count = 0;
				for (int x = ZhongShuOne.nStart; x <= ZhongShuOne.nEnd; x++)
				{
					//loop_count = loop_count + 1;
					//if (loop_count > loop_max)
					//{
					//	printf("FindZhongshu loop force break\n");
					//	break;
					//}
					if (pIn[x] == -1)
					{
						if (nLowCount == 0)
						{
							nLowCount++;
							fLowValue = pLow[x];
							nLowIndex = x;
							nNextLowIndex = x;
							nLowIndexCount++;
							//printf("nLowIndex:%d, nLowCount:%d\n, nLowIndexCount:%d", nLowIndex, nLowCount, nLowIndexCount);
						}
						else
						{
							nLowCount++;
							if (pLow[x] <= fLowValue)
							{
								if (nLowCount > 2)
								{
									bValid = true;
								}
								fLowValue = pLow[x];
								nLowIndex = x;
								nLowIndexCount++;
								nHighIndex = nHighIndexTemp;
							}
							else
							{
								if (nNextLowIndex == nLowIndex)
									{
										nNextLowIndex = x;
									}									
								}
							}
							//printf("nLowIndex:%d, nLastLowIndex:%d, nLowCount:%d\n, nLowIndexCount:%d", nLowIndex, nLastLowIndex, nLowCount, nLowIndexCount);
						}
						else if (pIn[x] == 1)
						{
							nHighIndexTemp = x;
						}
					}
					if (bValid)
					{
						ZhongShuOne.nEnd = nHighIndex; // 中枢结束点移到最高点的前一个低点。
					}
					
					
					if (nLowIndexCount > 1)
					{
						//int pre_i = i;
						i = nLowIndex - 1;
						//printf("nLowIndexCount:%d, use zs low index, i:%d => %d\n", nLowIndexCount, pre_i, i);
					}
					else
					{
						//int pre_i = i;
						i = nNextLowIndex - 1;
						//printf("nLowIndexCount:%d, use last low index, i:%d => %d\n", nLowIndexCount, pre_i, i);
						if (nNextLowIndex > minNextLowIndex - 1)
						{
							i = nNextLowIndex - 1;
							minNextLowIndex = i;
						}
						else
						{
							minNextLowIndex = minNextLowIndex + 1;
							i = nNextLowIndex + 1;
							printf("minNextLowIndex => %d\n", minNextLowIndex);
						}
					}
					
					//i = ZhongShuOne.nEnd - 1;
				}
				else
				{
					i = ZhongShuOne.nEnd - 1;
				}
				if (bValid)
				{
					// 进行中枢开始、结束的标记
					pRangeOut[ZhongShuOne.nStart + 1] = 1;
					pRangeOut[ZhongShuOne.nEnd - 1] = 2;

					// 区段内更新算得的中枢高、低、方向数据
					for (int j = ZhongShuOne.nStart + 1; j <= ZhongShuOne.nEnd - 1; j++)
					{
						pHighOut[j] = ZhongShuOne.fHigh;
						pLowOut[j] = ZhongShuOne.fLow;						
						pDirectionOut[j] = (float) ZhongShuOne.nDirection;
						
					}

				}

				ZhongShuOne.Reset();
			}
		}
	}
	if (ZhongShuOne.bValid)
	{
		// 进行中枢开始、结束的标记
		pRangeOut[ZhongShuOne.nStart + 1] = 1;
		pRangeOut[ZhongShuOne.nEnd - 1] = 2;

		// 区段内更新算得的中枢高、低、方向数据
		for (int j = ZhongShuOne.nStart + 1; j <= ZhongShuOne.nEnd - 1; j++)
		{
			pHighOut[j] = ZhongShuOne.fHigh;
			pLowOut[j] = ZhongShuOne.fLow;
			pDirectionOut[j] = (float) ZhongShuOne.nDirection;
		}
	}
	//printf("end FindZhongshu\n");
}



//=============================================================================
// 缠论K线
//=============================================================================
//__declspec(dllexport)
EXPORT void ChanK(
    float *pDirection, float *pInclude, float *pOutHigh, float *pOutLow,
    float *pHigh, float *pLow, int nCount
    )
{
    BaoHan(nCount, pDirection, pOutHigh, pOutLow, pInclude, pHigh, pLow);
}

//=============================================================================
// 缠论笔
//=============================================================================
//__declspec(dllexport) 
EXPORT void ChanBi(
    float *pBi,
    float *pHigh, float *pLow, int nCount,
    int iBi = 0, int iFenXingQuJian = 2
    )
{
    Func6(nCount, pBi, NULL, pHigh, pLow, iBi, iFenXingQuJian);
}

//=============================================================================
// 缠论线段
//=============================================================================
//__declspec(dllexport) 
EXPORT void ChanDuan(
    float *pDuan,
    float *pBi, float *pHigh, float *pLow, int nCount,
    int iDuan = 0
    )
{
    Func7(nCount, pDuan, pBi, pHigh, pLow, iDuan);

}


//=============================================================================
// 计算缠论中枢
// 输入: pDuan,线段队列， pHigh K线高点，pLow K线低点,
//=============================================================================
//__declspec(dllexport) 
EXPORT void ChanZhongShu(
    float *pZSHigh, float *pZSLow, float *pZSRange, float *pZSDirection,
    float *pDuan, float *pHigh, float *pLow, int nCount
    )
{
    // 寻找中枢得高点
	//printf("Find zhongshu high\n");
    Func8(nCount, pZSHigh, pDuan, pHigh, pLow);
    // 寻找中枢的低点
	//printf("Find zongshu low\n");
    Func9(nCount, pZSLow, pDuan, pHigh, pLow);
    // 寻找中枢的范围（开始时间、结束时间）
	//printf("Find zhongshu rage\n");
    Func10(nCount, pZSRange, pDuan, pHigh, pLow);
    // 寻找中枢的方向
	//printf("Find zhongshu direction\n");
    Func11(nCount, pZSDirection, pDuan, pHigh, pLow);
	
}

//=============================================================================
// 计算缠论中枢
// 输入: pDuan,线段队列， pHigh K线高点，pLow K线低点,
//=============================================================================
//__declspec(dllexport) 
EXPORT void ChanZhongShu2(
	float *pZSHigh, float *pZSLow, float *pZSRange, float *pZSDirection,
	float *pDuan, float *pHigh, float *pLow, int nCount
)
{
	FindZhongshu(nCount, pZSHigh, pZSLow, pZSRange, pZSDirection, pDuan, pHigh, pLow);
}


int main() {

}

}
