//
// Created by George Maged on 03.06.24.
//
#include "mmap_manager.h"
#include <iostream>

using namespace dbms;
using namespace std;

void Tuple::print() const {
    cout << "Tuple: " << _tid << " " << _a << " " << _b << endl;
}

void Tuple::setFromTuple(Tuple *t) {
    _tid = t->getTid();
    _a = t->getA();
    _b = t->getB();
}

void Header::print() const {
    cout << "Header: " << tuple_count << " " << pageLSN << endl;
}


Page::Page(const char *data) : header((Header *) data) {
    uint64_t headerSize = sizeof(header->tuple_count) + sizeof(header->pageLSN);
    uint64_t tupleSize = sizeof(Tuple);

    // Deserialize tuples
    uint64_t tupleCount = header->tuple_count;
    tuples.resize(tupleCount);
    for (uint64_t i = 0; i < tupleCount; ++i) {
        tuples[i] = (Tuple *) (data + headerSize + i * tupleSize);
    }
}

void Page::insert_tuple(Tuple *t) {
    header->tuple_count++;
    tuples.push_back(t);
}

std::pair<char *, size_t> Page::serialize() const {
    uint64_t headerSize = sizeof(header->tuple_count) + sizeof(header->pageLSN);
    uint64_t tupleSize = sizeof(Tuple);
    uint64_t dataSize = headerSize + tupleSize * tuples.size();

    char *data = new char[dataSize];

    // Serialize header
    std::memcpy(data, &header->tuple_count, sizeof(header->tuple_count));
    std::memcpy(data + sizeof(header->tuple_count), &header->pageLSN, sizeof(header->pageLSN));

    // Serialize tuples
    for (uint64_t i = 0; i < tuples.size(); ++i) {
        std::memcpy(data + headerSize + i * tupleSize, tuples[i], tupleSize);
    }

    return {data, dataSize};
}

void Page::print() {
    header->print();
    for (auto &i: tuples) {
        i->print();
    }
}

Tuple *Page::getTupleFromPage(uint64_t tid) {
    for (auto &i: tuples) {
        if (tid == i->getTid()) {
            return i;
        }
    }
}

void Page::updateLSN(uint64_t lsn) {
    this->header->pageLSN = lsn;
}

Tuple *MmapManager::read_tuple(uint64_t tid) {
    return this->get_page_by_tuple_id(tid)->getTupleFromPage(tid);
}

Page *MmapManager::get_page_by_tuple_id(uint64_t tid) {
    const uint64_t tupleSize = 24; // 3 bytes
    const uint64_t headerSize = 8; // 2 uint_64 -> 8 bytes

    const uint64_t tuplesPerPage = (this->pageSizeInBytes - headerSize) / tupleSize;

    size_t pageOffset = tid / tuplesPerPage;
    char *data = fileMapper->map_file(this->dbFilePath, pageOffset * pageSizeInBytes);
    return new Page(data);
}

MmapManager::MmapManager() : transactionId(1) {

    this->fileMapper = new FileMapper(this->pageSizeInBytes);
    this->walManager = new WalManager(this->pageSizeInBytes);
    uint64_t num_pages = get_num_pages();
    cout << num_pages << endl;
    for (uint64_t i=0;i<num_pages;i++){
        Page* p = get_page(i);
        assert(p->getHeader()->pageLSN <= walManager->flushedLSN);
    }
}

MmapManager::~MmapManager() {
    cout << "Destroying MmapManager " << endl;
    uint64_t dbFileSize = fileMapper->get_file_size(dbFilePath);
    cout << dbFileSize << endl;
}

void MmapManager::update_tuple(Tuple *newTuple) {
    uint64_t tid = newTuple->getTid();

    Page *p = get_page_by_tuple_id(tid);

    // BEGIN Transaction
    auto *begin = new LogRecord(transactionId, TransactionState::BEGIN, newTuple, newTuple);

    uint64_t latestLSN = this->walManager->appendRecord(begin);

    p->updateLSN(latestLSN);
    Tuple *oldTuple = p->getTupleFromPage(tid);

    // update the tuple
    // save old copy in the LogFile
    auto *oldTupleCopy = new Tuple(*oldTuple);
    p->getTupleFromPage(tid)->setFromTuple(newTuple);
    auto *running = new LogRecord(transactionId, TransactionState::RUNNING, oldTupleCopy, newTuple);
    latestLSN = this->walManager->appendRecord(running);
    p->updateLSN(latestLSN);

    // Commit Txn
    auto *commit = new LogRecord(transactionId, TransactionState::COMMIT, newTuple, newTuple);
    latestLSN = this->walManager->appendRecord(commit);
    p->updateLSN(latestLSN);

    transactionId++;
}

Page *MmapManager::get_page(uint64_t pageId) {
    char *data = fileMapper->map_file(this->dbFilePath, pageId * pageSizeInBytes);
    return new Page(data);
}


uint64_t dbms::WalManager::appendRecord(dbms::LogRecord *logRecord) {

    logRecord->lsn = nextLSN();
    records.push_back(logRecord);
    return logRecord->lsn;
}

dbms::WalManager::WalManager(size_t pageSizeInBytes) {
    latch.lock();
    LSN = 0;

    fileMapper = new FileMapper(pageSizeInBytes);
    char *lsnValue = fileMapper->map_file(flushedLSNFilePath, 0);
    std::memcpy(&LSN, lsnValue, sizeof(uint64_t));

    flushedLSN = LSN;
    latch.unlock();
}

uint64_t dbms::WalManager::nextLSN() {
    return ++LSN;
}

void WalManager::print() {
    cout << "LATEST LSN: " << LSN << endl;
    cout << "Number of LogRecords : " << records.size() << endl;
    for (auto &i: records) {
        cout << "LogRecordLSN: " << i->lsn << " TransactionID: " << i->transactionId << " State: " << i->state << endl;
        if (i->state == TransactionState::RUNNING)
            i->oldTuple->print();

        if (i->state == TransactionState::RUNNING)
            i->newTuple->print();
    }
}

void WalManager::flushWal() {
    latch.lock();

    std::pair<char *, size_t> walData = this->serialize();

    if (walData.second != 0) {
        dbms::FileMapper::append_to_file("WAL.txt", walData.first, walData.second);
        uint64_t lsnValue = this->LSN;

        char *lsnInBytes = new char[sizeof(uint64_t)];
        memcpy(lsnInBytes, &lsnValue, sizeof(uint64_t));

        flushedLSN = lsnValue;
        latch.unlock();
        fileMapper->write_to_file(flushedLSNFilePath, lsnInBytes, sizeof(uint64_t));
        cout << "Flushed LSN " << this->LSN << endl;
    } else {
        latch.unlock();
    }
}

std::pair<char *, size_t> WalManager::serialize() {
    uint64_t logRecordSize = sizeof(TransactionState) + sizeof(uint64_t) * 2 + sizeof(Tuple) * 2;

    uint64_t dataSize = logRecordSize * records.size();
    char *data = new char[dataSize];

    // Serialize tuples
    for (uint64_t i = 0; i < records.size(); ++i) {
        std::memcpy(data + i * logRecordSize, records[i], logRecordSize);
    }

    return {data, dataSize};
}


dbms::LogRecord::LogRecord(uint64_t transactionId, dbms::TransactionState state, dbms::Tuple *oldTuple,
                           dbms::Tuple *newTuple) : transactionId(transactionId), state(state), oldTuple(oldTuple),
                                                    newTuple(newTuple) {

}

