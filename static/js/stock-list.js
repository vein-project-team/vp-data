{
    const stockListBean = {
        delimiters: ['[[', ']]'],
        data() {
            return {
                stock_list: []
            };
        },
        mounted() {
            axios.get(`/stock-list-data?exchange=sh&page=1`)
                .then((response) => {
                    this.stock_list.push(response.data['stocks']);
                    changeContainerHeight('');
                }).catch(function (error) {
                    console.log(error);
                });
        }
    }
    Vue.createApp(stockListBean).mount(".central-box");
}