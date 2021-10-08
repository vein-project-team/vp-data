{
    const stockListBean = {
        delimiters: ['[[', ']]'],
        data() {
            return {
                stock_list: []
            };
        },
        mounted() {
            let page = 1;
            axios.get(`/stock-list-data?exchange=sh&page=${page}`)
                .then((response) => {
                    response.data['stocks'].length = 100;
                    this.stock_list.push(response.data['stocks']);
                    changeContainerHeight('');
                }).catch(function (error) {
                    console.log(error);
                });
        }
    }
    Vue.createApp(stockListBean).mount(".central-box");
}