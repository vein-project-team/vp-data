{
    const stockListBean = {
        delimiters: ['[[', ']]'],
        data() {
            return {
                page: 1,
                stockDisplayInAPage: 30,
                minPage: 1,
                maxPage: 1,
                sortKey: '',
                sortInd: 1,
                rawStockList: [],
                resultStockList: [],
                displayStockList: [],
                searchString: ''
            };
        },
        mounted() {
            let page = 1;
            axios.get(`/stock-list-data?exchange=sh&page=${page}`)
                .then((response) => {
                    for (let i = 0; i < response.data['stocks'].length; i++)
                        this.rawStockList.push(response.data['stocks'][i]);
                    this.resetResultStockList();
                    changeContainerHeight('');
                }).catch(function (error) {
                    console.log(error);
                });
        },
        methods: {
            resetResultStockList(filter=null) {
                this.resultStockList = this.rawStockList.slice(0);
                if (filter != null) {
                    this.page = 1;
                    this.resultStockList = this.resultStockList.filter(filter);
                }
                this.maxPage = Math.floor(this.resultStockList.length / this.stockDisplayInAPage) + 1;
                if (this.resultStockList.length == 1) {
                    window.open(`/stock-quotation?stock=${this.resultStockList[0]['ts_code']}`, '_self');
                }
                else {
                    this.resetDisplayStockList();
                }
            },
            resetDisplayStockList() {
                if (this.page == this.maxPage) {
                    this.displayStockList = this.resultStockList.slice((this.page-1)*this.stockDisplayInAPage, this.resultStockList.length);
                }
                else {
                    this.displayStockList = this.resultStockList.slice((this.page-1)*this.stockDisplayInAPage, this.page*this.stockDisplayInAPage);
                }
                changeContainerHeight('');
            },
            sortBy(key) {
                if (this.sortKey != key) {
                    this.sortKey = key;
                    this.sortInd = 1;
                }
                else {
                    this.sortInd *= -1;
                }
                this.resultStockList.sort((x, y) => {
                    if (x[key] == 'NULL' && y[key] == 'NULL')
                        return 0;
                    else if (x[key] == 'NULL')
                        return 1;
                    else if (y[key] == 'NULL')
                        return -1;
                    else if (x[key].indexOf('退') > -1 && y[key].indexOf('退') > -1)
                        return 0;
                    else if (x[key].indexOf('退') > -1)
                        return 1;
                    else if (y[key].indexOf('退') > -1)
                        return -1;
                    else if (x[key].indexOf('*ST') > -1 && y[key].indexOf('*ST') > -1)
                        return 0;
                    else if (x[key].indexOf('*ST') > -1)
                        return 1;
                    else if (y[key].indexOf('*ST') > -1)
                        return -1;
                    else if (x[key].indexOf('ST') > -1 && y[key].indexOf('ST') > -1)
                        return 0;
                    else if (x[key].indexOf('ST') > -1)
                        return 1;
                    else if (y[key].indexOf('ST') > -1)
                        return -1;
                    return this.sortInd*x[key].localeCompare(y[key], 'zh');
                });
                this.page = 1;
                this.resetDisplayStockList();
            },
            search() {
                this.resetResultStockList((item) => {
                   let searchIn = item['ts_code'] + item['name'] + item['area'] + item['industry'] + item['list_date'];
                   return searchIn.indexOf(this.searchString) > -1;
                });
            },
            changePage(value, event=null) {
                if (value == 'pre') {
                    if (this.page == this.minPage)
                        return;
                    this.page = this.page - 1;
                }
                else if (value == 'nex') {
                    if (this.page == this.maxPage)
                        return;
                    this.page = this.page + 1;
                }
                else if (value == 'input') {
                    let newPage = event.currentTarget.value;
                    this.page = newPage;
                }
                else {
                    if (this.page == value)
                        return;
                    this.page = value;
                }
                if (isNaN(this.page))
                    return;
                else if (this.page > this.maxPage)
                    this.page = this.maxPage
                else if (this.page < this.minPage)
                    this.page = this.minPage
                this.resetDisplayStockList()
            }
        }
    }
    Vue.createApp(stockListBean).mount(".central-box");
}