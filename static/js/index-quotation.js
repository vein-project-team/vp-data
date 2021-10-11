{
    const indexBean = {
        delimiters: ['[[', ']]'],
        data() {
            return {
                currentIndex: 'SH',
                currentIndexName: '上证指数',
                SH: {
                    'daily': null,
                    'weekly': null
                },
                SZ: {
                    'daily': null,
                    'weekly': null
                },
            }
        },
        mounted() {
            this.drawIndexGraph('上证指数', 'SH', 'daily');
            this.drawIndexGraph('上证指数', 'SH', 'weekly');
            this.drawIndexGraph('上证指数', 'SH', 'monthly');
        },
        methods: {
            changeCurrentIndexTo(indexName, indexSuffix) {
                this.currentIndex = indexSuffix;
                this.currentIndexName = indexName;
                this.drawIndexGraph(indexName, indexSuffix, 'daily');
                this.drawIndexGraph(indexName, indexSuffix, 'weekly');
                this.drawIndexGraph(indexName, indexSuffix, 'monthly');
            },
            isCurrent(id) {
                return id == this.currentIndex;
            },
            drawIndexGraph(indexName, indexSuffix, frequency) {
                let quotationChart = echarts.init(document.querySelector(`.index-${frequency}-chart`));
                if (this[indexSuffix][frequency] == null) {
                    axios.get(`/index-quotation-data?index=${indexSuffix}`)
                        .then((response) => {
                            let date = response.data[indexSuffix][frequency]['date'];
                            let kLine = response.data[indexSuffix][frequency]['k_line'];
                            let volRaw = response.data[indexSuffix][frequency]['vol'];
                            let kMa30 = response.data[indexSuffix][frequency]['k_ma30'];
                            let volMa30 = response.data[indexSuffix][frequency]['vol_ma30'];
                            let ups = response.data[indexSuffix][frequency]['ups'];
                            let downs = response.data[indexSuffix][frequency]['downs'];
                            let adLine = response.data[indexSuffix][frequency]['ad_line'];

                            let vol = []
                            for (let i = 0; i < volRaw.length; i++) {
                                vol.push([date[i], volRaw[i], kLine[i][0] > kLine[i][1] ? 1 : -1]);
                            }

                            let adLineOffsets = []
                            for (let i = 0; i < adLine.length; i++) {
                                let adLineOffset = ups[i] - downs[i];
                                adLineOffsets.push([date[i], adLineOffset, adLineOffset >= 0 ? 1 : -1]);
                            }

                            let quotationChartOption = {

                                tooltip: {
                                    trigger: 'axis',
                                    axisPointer: {
                                        type: 'cross',
                                        link: {xAxisIndex: 'all'},
                                        snap: true
                                    },
                                    textStyle: {
                                        color: '#000'
                                    },
                                    formatter: function (params) {
                                        if (params[0].componentSubType == "candlestick") {
                                            let kLine = params[0];
                                            let date = params[0].name;
                                            let currentData = kLine.data;
                                            return `
                                            <span class="axis-label-date">${kLine.name}</span>
                                            <ul style="padding-left: 15px;">
                                                <li>开盘：${currentData[1]}</li>
                                                <li>收盘：${currentData[2]}</li>
                                                <li>最低：${currentData[3]}</li>
                                                <li>最高：${currentData[4]}</li>
                                            </ul>`;
                                        } else if (params[0].componentSubType == "bar") {
                                            let date = params[2].name;
                                            let vol = params[0].data[1];
                                            return `
                                            <span class="axis-label-date">${date}</span>
                                            <ul style="padding-left: 15px;">
                                                <li>成交量：${vol}</li>
                                            </ul>
                                            `;
                                        } else {
                                            let date = params[2].name;
                                            let adPoint = params[0].data;
                                            let adOffset = params[1].data[1];
                                            return `
                                            <span class="axis-label-date">${date}</span>
                                            <ul style="padding-left: 15px;">
                                                <li>腾落线：${adPoint}</li>
                                                <li>变动量：${adOffset}</li>
                                            </ul>
                                            `;
                                        }
                                    }
                                },
                                axisPointer: {
                                    link: {xAxisIndex: 'all'},
                                    label: {
                                        backgroundColor: '#777'
                                    }
                                },
                                visualMap: [
                                    {
                                        show: false,
                                        seriesIndex: 1,
                                        pieces: [{
                                            value: 1,
                                            color: '#aa0000'
                                        }, {
                                            value: -1,
                                            color: '#00aa00'
                                        }]
                                    },
                                    {
                                        show: false,
                                        seriesIndex: 5,
                                        pieces: [{
                                            value: 1,
                                            color: '#aa0000'
                                        }, {
                                            value: -1,
                                            color: '#00aa00'
                                        }]
                                    },
                                ],
                                grid: [
                                    {
                                        top: 20,
                                        left: '6%',
                                        right: '4%',
                                        height: 360
                                    },
                                    {
                                        top: 380,
                                        left: '6%',
                                        right: '4%',
                                        height: 120
                                    },
                                    {
                                        top: 500,
                                        left: '6%',
                                        right: '4%',
                                        height: 120
                                    }
                                ],
                                xAxis: [
                                    {
                                        type: 'category',
                                        show: false,
                                        data: date,
                                        axisPointer: {
                                            label: {
                                                show: false
                                            }
                                        },
                                    },
                                    {
                                        type: 'category',
                                        show: false,
                                        data: date,
                                        gridIndex: 1,
                                        axisPointer: {
                                            label: {
                                                show: false
                                            }
                                        }
                                    },
                                    {
                                        type: 'category',
                                        gridIndex: 2,
                                        data: date,
                                        axisLabel: {
                                            interval: 10,
                                            showMaxLabel: true,
                                            showMinLabel: true
                                        }
                                    }
                                ],
                                yAxis: [
                                    {
                                        scale: true,
                                        splitArea: {
                                            show: true
                                        }
                                    },
                                    {
                                        scale: true,
                                        gridIndex: 1,
                                        axisLabel: {show: false},
                                        axisLine: {show: false},
                                        axisTick: {show: false},
                                        splitLine: {show: false}
                                    },
                                    {
                                        show: false,
                                        scale: true,
                                        gridIndex: 2,
                                    },
                                ],
                                series: [{
                                    type: 'candlestick',
                                    data: kLine,
                                    barWidth: 7,
                                    xAxisIndex: 0,
                                    yAxisIndex: 0,
                                    itemStyle: {
                                        color: '#00aa00',
                                        color0: '#aa0000',
                                        borderColor: null,
                                        borderColor0: null
                                    },
                                },
                                    {
                                        type: 'bar',
                                        data: vol,
                                        barWidth: 7,
                                        xAxisIndex: 1,
                                        yAxisIndex: 1,
                                    },
                                    {
                                        type: 'line',
                                        showSymbol: false,
                                        data: kMa30,
                                        smooth: true,
                                        xAxisIndex: 0,
                                        yAxisIndex: 0,
                                        lineStyle: {
                                            color: 'darkblue'
                                        }
                                    },
                                    {
                                        type: 'line',
                                        showSymbol: false,
                                        data: volMa30,
                                        smooth: true,
                                        xAxisIndex: 1,
                                        yAxisIndex: 1,
                                        lineStyle: {
                                            color: 'darkblue'
                                        }
                                    },
                                    {
                                        type: 'line',
                                        showSymbol: false,
                                        xAxisIndex: 2,
                                        yAxisIndex: 2,
                                        data: adLine,
                                        lineStyle: {
                                            color: 'darkblue'
                                        }
                                    },
                                    {
                                        type: 'bar',
                                        xAxisIndex: 2,
                                        yAxisIndex: 2,
                                        data: adLineOffsets
                                    }
                                ]
                            };

                            let data = {
                                quotationChartOption: quotationChartOption,
                            };

                            this[indexSuffix][frequency] = data
                            quotationChart.setOption(data['quotationChartOption']);

                        })
                        .catch(function (error) {
                            console.log(error);
                        });
                } else {
                    quotationChart.setOption(this[indexSuffix][frequency]['quotationChartOption']);
                }
            }
        }
    }
    Vue.createApp(indexBean).mount(".container");

}