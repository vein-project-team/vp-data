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
                    clearChart(`#SH`);
                    clearChart(`#SZ`);
                    drawIndexGraph("上证指数", '000001.SH', days, 'SH');
                    drawIndexGraph("深证指数", '399001.SZ', days, 'SZ');
                }
            });
        }
    };

    function clearChart(path) {
        document.querySelector(path).innerHTML = '';
    }

    function drawIndexGraph(indexName, indexCode, days, domID) {
        document.querySelector(`#${domID}`).removeAttribute("_echarts_instance_");
        let chart = echarts.init(document.querySelector(`#${domID}`));
        axios.get(`/index-daily?code=${indexCode}&days=${days}`)
        .then(function (response) {
            let date = response.data[indexCode]['date'];
            let data = response.data[indexCode]['data'];
            let ma = response.data[indexCode]['ma30'];
            let option = {
                tooltip: {
                    trigger: 'axis',
                    axisPointer: { type: 'cross' }
                },
                xAxis: {
                    type: 'category',
                    data: date,
                },
                yAxis: {
                    scale: true,
                },
                series: [
                    {
                        data: data,
                        type: 'line',
                        name: indexName,
                    },
                    {
                        data: ma,
                        type: 'line',
                        name: "均线",
                        smooth: true
                    },
                ]
            };
            chart.setOption(option);
        })
        .catch(function (error) {
            console.log(error);
        });
    }

    headerBean.onload();

}