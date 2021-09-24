{
    const indexBean = {
        data() {
            return {
                currentIndex: 'SH',
                SH: null,
                SZ: null,
            }
        },
        mounted() {
            this.drawIndexGraph('上证指数', 'SH', 120);
        },
        methods: {
            changeCurrentIndexTo(indexName, indexSuffix) {
                this.currentIndex = indexSuffix;
                this.drawIndexGraph(indexName, indexSuffix, 120);
            },
            isCurrent(id) {
                return id == this.currentIndex;
            },
            drawIndexGraph(indexName, indexSuffix, days) {
                let indexDailyChart = echarts.init(document.querySelector(`.index-daily-chart`));
                let adLineChart = echarts.init(document.querySelector(`.index-ad-line-chart`));
                if (this[indexSuffix] == undefined) {
                    axios.get(`/index-daily-data?index=${indexSuffix}&days=${days}`)
                        .then((response) => {
                            let date = response.data[indexSuffix]['date'];
                            let kLine = response.data[indexSuffix]['k_line'];
                            let volRaw = response.data[indexSuffix]['vol'];
                            let kMa30 = response.data[indexSuffix]['k_ma30'];
                            let volMa30 = response.data[indexSuffix]['vol_ma30'];
                            let adLine = response.data[indexSuffix]['ad_line'];

                            let vol = []
                            for (let i = 0; i < kLine.length; i++) {
                                vol.push([date[i], volRaw[i], kLine[i][0] > kLine[i][1] ? 1 : -1]);
                            }

                            let indexDailyChartOption = {

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
                                            </ul>`
                                        } else if (params[0].componentSubType == "bar") {
                                            let date = params[2].name;
                                            let vol = params[0].data[1];
                                            return `
                                            <span class="axis-label-date">${date}</span>
                                            <ul style="padding-left: 15px;">
                                                <li>成交量：${vol}</li>
                                            </ul>
                                            `
                                        }
                                    }
                                },
                                axisPointer: {
                                    link: {xAxisIndex: 'all'},
                                    label: {
                                        backgroundColor: '#777'
                                    }
                                },
                                visualMap: {
                                    show: false,
                                    seriesIndex: 1,
                                    pieces: [{
                                        value: 1,
                                        color: '#00aa00'
                                    }, {
                                        value: -1,
                                        color: '#aa0000'
                                    }]
                                },
                                grid: [
                                    {
                                        top: "5%",
                                        left: '6.5%',
                                        right: '3%',
                                        height: '60%'
                                    },
                                    {
                                        left: '6%',
                                        right: '3%',
                                        top: '66%',
                                        height: '16%'
                                    }
                                ],
                                xAxis: [
                                    {
                                        type: 'category',
                                        show: false,
                                        data: date,
                                        scale: true,
                                        splitNumber: 20,
                                        min: 'dataMin',
                                        max: 'dataMax',
                                        axisPointer: {
                                            label: {
                                                show: false
                                            }
                                        },
                                    },
                                    {
                                        type: 'category',
                                        gridIndex: 1,
                                        data: date,
                                        scale: true,
                                        splitNumber: 20,
                                        min: 'dataMin',
                                        max: 'dataMax'
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
                                        splitNumber: 2,
                                        axisLabel: {show: false},
                                        axisLine: {show: false},
                                        axisTick: {show: false},
                                        splitLine: {show: false}
                                    }
                                ],

                                series: [{
                                    type: 'candlestick',
                                    data: kLine,
                                    barWidth: 7,
                                    itemStyle: {
                                        color: '#aa0000',
                                        color0: '#00aa00',
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
                                        name: "MA30",
                                        smooth: true,
                                        lineStyle: {
                                            color: 'darkblue'
                                        }
                                    },
                                    {
                                        type: 'line',
                                        showSymbol: false,
                                        data: volMa30,
                                        name: "MA30",
                                        smooth: true,
                                        xAxisIndex: 1,
                                        yAxisIndex: 1,
                                        lineStyle: {
                                            color: 'darkblue'
                                        }
                                    }
                                ]
                            };

                            let adLineChartOption = {
                                grid: {
                                    top: '10px',
                                    left: '60px',
                                    right: '40px',
                                    bottom: '30px'
                                },
                                xAxis: {
                                    data: date
                                },
                                yAxis: {
                                    show: false,
                                    scale: true,
                                },
                                series: [
                                    {
                                        type: 'line',
                                        showSymbol: false,
                                        data: adLine,
                                    }
                                ]
                            };

                            let data = {
                                indexDailyChartOption: indexDailyChartOption,
                                adLineChartOption: adLineChartOption
                            };

                            this[indexSuffix] = data
                            indexDailyChart.setOption(data['indexDailyChartOption']);
                            adLineChart.setOption(data['adLineChartOption']);

                        })
                        .catch(function (error) {
                            console.log(error);
                        });
                } else {
                    indexDailyChart.setOption(this[indexSuffix]['indexDailyChartOption']);
                    adLineChart.setOption(this[indexSuffix]['adLineChartOption']);
                }
            }
        }
    }
    Vue.createApp(indexBean).mount(".container");

}