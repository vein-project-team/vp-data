{
    const stockListBean = {
        delimiters: ['[[', ']]'],
        data() {
            return {
                page: 1,
                stockDisplayInAPage: 30,
                stockList: [],
                displayStockList: [],
                searchString: ''
            };
        },
        mounted() {
            let page = 1;
            axios.get(`/stock-list-data?exchange=sh&page=${page}`)
                .then((response) => {
                    for (let i = 0; i < response.data['stocks'].length; i++)
                        this.stockList.push(response.data['stocks'][i]);
                    this.displayStockList = this.stockList.slice((this.page-1)*this.stockDisplayInAPage, this.page*this.stockDisplayInAPage-1),
                    changeContainerHeight('');
                }).catch(function (error) {
                    console.log(error);
                });
        },
        methods: {
            search() {
                window.open(`/stock-quotation?stock=${this.searchString}`, '_self')
            },
            changePage(ind, event=null) {
                if (ind == 0) {
                    this.page = event.currentTarget.value;
                }
                else if (ind == -1) {
                    this.page = this.page == 1 ? 10 : this.page - 1;
                }
                else if (ind == 1) {
                    this.page = this.page == 10 ? 1 : this.page + 1;
                }
                console.log(this.page)
                this.displayStockList = this.stockList.slice((this.page-1)*this.stockDisplayInAPage, this.page*this.stockDisplayInAPage-1);
            }
        }
    }
    Vue.createApp(stockListBean).mount(".central-box");
}