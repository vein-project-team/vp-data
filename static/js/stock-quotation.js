{
    let stockQuotationBean = {
        delimiters: ['[[', ']]'],
        data() {
            return {
                stock: stock,
            }
        },
        mounted() {
            this.drawQuotationChart(stock, 'DAILY');
            this.drawQuotationChart(stock, 'WEEKLY');
            this.drawQuotationChart(stock, 'MONTHLY');
        },
        methods: {
            drawQuotationChart(stock, frequency) {
                let quotationChart = echarts.init(document.querySelector(`.${frequency.toLowerCase()}-chart`));
                axios.get(`/stock-quotation-data?stock=${stock}&frequency=${frequency}`)
                    .then((response) => {
                        let date = response.data[stock]['date'];
                        let kLine = response.data[stock]['k_line'];
                        let volRaw = response.data[stock]['vol'];
                        let kMa30 = response.data[stock]['k_ma30'];
                        let volMa30 = response.data[stock]['vol_ma30'];

                        let vol = []
                        for (let i = 0; i < volRaw.length; i++) {
                            vol.push([date[i], volRaw[i], kLine[i][0] > kLine[i][1] ? 1 : -1]);
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
                                        color: '#00aa00'
                                    }, {
                                        value: -1,
                                        color: '#aa0000'
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
                                    data: date,
                                    gridIndex: 1,
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
                                }
                            ],
                            series: [{
                                type: 'candlestick',
                                connectNulls: true,
                                data: kLine,
                                barWidth: 7,
                                xAxisIndex: 0,
                                yAxisIndex: 0,
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
                                }
                            ]
                        };

                        quotationChart.setOption(quotationChartOption);

                    })
                    .catch(function (error) {
                        console.log(error);
                    });
            }
        }
    }
    Vue.createApp(stockQuotationBean).mount(".central-box");
}