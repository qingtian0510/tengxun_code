
def findMedianSortedArrays(nums1, nums2):
    all_num = []
    ind_1 = ind_2 = 0
    while ind_1 < len(nums1) and ind_2 < len(nums2):
        if nums1[ind_1] < nums2[ind_2]:
            all_num.append(nums1[ind_1])
            ind_1 += 1
        else:
            all_num.append(nums2[ind_2])
            ind_2 += 1

    if ind_1 == len(nums1):
        all_num.extend(nums2[ind_2:])
    else:
        all_num.extend(nums1[ind_1:])

    if len(all_num) == 0:
        return None
    if len(all_num)%2 == 0:
        return (all_num[int(len(all_num)/2)-1] + all_num[int(len(all_num)/2)])/2
    else:
        return all_num[len(all_num)//2]

num1 = [1,2]
num2 = [3,4]

print(findMedianSortedArrays(num1, num2))