{
    let headerBean = {
        daysInput: document.querySelector('#daysInput'),
        daysInputSubmit: document.querySelector('#daysInputSubmit'),
        onload: function () {

            drawIndexGraph("上证指数", '000001.SH', 200, 'SH');
            drawIndexGraph("深证指数", '399001.SZ', 200, 'SZ');

            let keeper = this;
            this.daysInputSubmit.addEventListener('click', function (event) {
                let days = keeper.daysInput.value;
                if (isNaN(days))
                    return;
                if (days < 5 || days > 500)
                    return;
                else {
                    drawIndexGraph("上证指数", '000001.SH', days, 'SH');
                    drawIndexGraph("深证指数", '399001.SZ', days, 'SZ');
                }
            });
        }
    };

    function drawIndexGraph(indexName, indexCode, days, domID) {
        document.querySelector(`#${domID}`).removeAttribute("_echarts_instance_");
        let dailyChart = echarts.init(document.querySelector(`#${domID} > .index-graph`));
        let volChart = echarts.init(document.querySelector(`#${domID} > .index-vol-graph`));
        let adlineChart = echarts.init(document.querySelector(`#${domID} > .index-ad-line`));
        axios.get(`/index-daily-data?code=${indexCode}&days=${days}`)
            .then(function (response) {
                let date = response.data[indexCode]['date'];
                let data = response.data[indexCode]['close'];
                let ma = response.data[indexCode]['ma30'];
                let vol = response.data[indexCode]['vol'];
                let adLine = response.data[indexCode]['adline'];

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
                            data: data,
                            type: 'line',
                            name: indexName,
                            color: '#aaa'
                        },
                        {
                            data: ma,
                            type: 'line',
                            name: "均线",
                            smooth: true
                        },
                    ]
                };

                let adlineChartOption = {
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
                        }
                    ]
                };

                dailyChart.setOption(dailyChartOption);
                volChart.setOption(volChartOption);
                adlineChart.setOption(adlineChartOption);

            })
            .catch(function (error) {
                console.log(error);
            });
    }

    headerBean.onload();

}