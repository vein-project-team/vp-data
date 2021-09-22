{
    let headerBean = {
        // daysInput: document.querySelector('#daysInput'),
        // daysInputSubmit: document.querySelector('#daysInputSubmit'),
        onload: function () {

            drawIndexGraph("上证指数", 'SH', 200);
            drawIndexGraph("深证指数", 'SZ', 200);

            // let keeper = this;
            // this.daysInputSubmit.addEventListener('click', function (event) {
            //     let days = keeper.daysInput.value;
            //     if (isNaN(days))
            //         return;
            //     if (days < 5 || days > 500)
            //         return;
            //     else {
            //         drawIndexGraph("上证指数", 'SH', days);
            //         drawIndexGraph("深证指数", 'SZ', days);
            //     }
            // });
        }
    };

    function drawIndexGraph(indexName, indexSuffix, days) {
        document.querySelector(`#${indexSuffix}`).removeAttribute("_echarts_instance_");
        let dailyChart = echarts.init(document.querySelector(`#${indexSuffix} > .index-graph`));
        let volChart = echarts.init(document.querySelector(`#${indexSuffix} > .index-vol-graph`));
        let adLineChart = echarts.init(document.querySelector(`#${indexSuffix} > .index-ad-line`));
        axios.get(`/index-daily-data?index=${indexSuffix}&days=${days}`)
            .then(function (response) {
                let date = response.data[indexSuffix]['date'];
                let k_line = response.data[indexSuffix]['k_line'];
                let vol = response.data[indexSuffix]['vol'];
                let k_ma30 = response.data[indexSuffix]['k_ma30'];
                let vol_ma30 = response.data[indexSuffix]['vol_ma30'];
                let adLine = response.data[indexSuffix]['ad_line'];

                let dailyChartOption = {
                    grid: {
                        top: '20px',
                        left: '60px',
                        right: '40px',
                        bottom: '10px'
                    },
                    tooltip: {
                        trigger: 'axis',
                        axisPointer: {type: 'cross'}
                    },
                    xAxis: {
                        show: false,
                        data: date
                    },
                    yAxis: {
                        scale: true,
                        axisLine: {
                            show: true
                        }
                    },
                    series: [
                        {
                          type: 'candlestick',
                          data: k_line
                        },
                        {
                            type: 'line',
                            data: k_ma30,
                            name: "MA30",
                            smooth: true
                        },
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
                            data: adLine,
                        }
                    ]
                };

                let volChartOption = {
                    grid: {
                        top: '0px',
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
                            type: 'bar',
                            data: vol,
                            color: '#aaa'
                        },
                        {
                            type: 'line',
                            data: vol_ma30,
                            name: "MA30",
                            smooth: true
                        },
                    ]
                };

                dailyChart.setOption(dailyChartOption);
                volChart.setOption(volChartOption);
                adLineChart.setOption(adLineChartOption);

            })
            .catch(function (error) {
                console.log(error);
            });
    }

    headerBean.onload();

}