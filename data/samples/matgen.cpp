#include <bits/stdc++.h>

using namespace std;

int main() {
  srand(time(0));
  int n;
  cin >> n;
  for (int i = 0; i < n; ++i) {
    cout << rand() % 100;
    for (int j = 1; j < n; ++j) {
      cout << "," << rand() % 100;
    }
    cout << endl;
  }
}