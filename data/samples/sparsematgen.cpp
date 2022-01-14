#include <bits/stdc++.h>

using namespace std;

bool prob() {
  int num = rand() % 100;
  if (num >= 80) {
    return true;
  }
  return false;
}

int main() {
  srand(time(0));
  int n;
  cin >> n;
  for (int i = 0; i < n; ++i) {
    if (prob()) {
      cout << 1 + (rand() % 100);
    } else {
      cout << 0;
    }
    for (int j = 1; j < n; ++j) {
      if (prob()) {
        cout << "," << 1 + (rand() % 100);
      } else {
        cout << "," << 0;
      }
    }
    cout << endl;
  }
}